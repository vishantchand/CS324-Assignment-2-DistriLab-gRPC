package fj.ac.usp.cs324.distrilab.worker;

import fj.ac.usp.cs324.distrilab.common.model.WorkerInfo;
import fj.ac.usp.cs324.distrilab.common.rmi.BootstrapService;
import fj.ac.usp.cs324.distrilab.common.rmi.WorkerService;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class WorkerServer {

    private static final String BOOTSTRAP_BINDING =
            "BootstrapService";

    private static final long NEIGHBOR_CHECK_INTERVAL_SECONDS = 5;

    private WorkerServer() {
    }

    public static void main(String[] args) {
        if (args.length != 5) {
            printUsage();
            return;
        }

        try {
            int workerId = parseInteger(
                    args[0],
                    "worker ID"
            );

            String host = args[1];

            int workerPort = parsePort(
                    args[2],
                    "worker registry port"
            );

            String bootstrapHost = args[3];

            int bootstrapPort = parsePort(
                    args[4],
                    "bootstrap registry port"
            );

            String bindingName = "Worker-" + workerId;

            System.setProperty(
                    "java.rmi.server.hostname",
                    host
            );

            WorkerInfo workerInfo = new WorkerInfo(
                    workerId,
                    host,
                    workerPort,
                    bindingName
            );

            Registry workerRegistry =
                    LocateRegistry.createRegistry(workerPort);

            WorkerServiceImpl workerService =
                    new WorkerServiceImpl(workerInfo);

            workerRegistry.rebind(
                    bindingName,
                    workerService
            );

            ScheduledExecutorService neighborMonitor =
                    createNeighborMonitor(
                            workerId,
                            workerService
                    );

            Registry bootstrapRegistry =
                    LocateRegistry.getRegistry(
                            bootstrapHost,
                            bootstrapPort
                    );

            BootstrapService bootstrapService =
                    (BootstrapService) bootstrapRegistry.lookup(
                            BOOTSTRAP_BINDING
                    );

            connectToExistingWorker(
                    workerInfo,
                    workerService,
                    bootstrapService
            );

            bootstrapService.registerWorker(workerInfo);

            Runtime.getRuntime().addShutdownHook(
                    new Thread(() -> {
                        neighborMonitor.shutdownNow();

                        notifyNeighborsOfShutdown(
                                workerInfo,
                                workerService
                        );

                        try {
                            bootstrapService.unregisterWorker(
                                    workerId
                            );
                        } catch (Exception exception) {
                            System.err.println(
                                    "Unable to unregister worker: "
                                            + exception.getMessage()
                            );
                        }
                    })
            );

            System.out.println(
                    "Worker Node started successfully."
            );
            System.out.println(
                    "Worker ID: " + workerId
            );
            System.out.println(
                    "Host: " + host
            );
            System.out.println(
                    "Registry port: " + workerPort
            );
            System.out.println(
                    "Binding name: " + bindingName
            );
            System.out.println(
                    "Connected to Bootstrap Node at "
                            + bootstrapHost
                            + ":"
                            + bootstrapPort
            );
            System.out.println(
                    "Checking neighbour availability every "
                            + NEIGHBOR_CHECK_INTERVAL_SECONDS
                            + " seconds."
            );

            new CountDownLatch(1).await();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            System.err.println(
                    "Worker Node was interrupted."
            );
        } catch (Exception exception) {
            System.err.println(
                    "Unable to start Worker Node: "
                            + exception.getMessage()
            );
            exception.printStackTrace();
        }
    }

    private static ScheduledExecutorService createNeighborMonitor(
            int workerId,
            WorkerServiceImpl workerService
    ) {
        ScheduledExecutorService neighborMonitor =
                Executors.newSingleThreadScheduledExecutor(
                        runnable -> {
                            Thread thread = new Thread(
                                    runnable,
                                    "neighbor-monitor-" + workerId
                            );

                            thread.setDaemon(true);
                            return thread;
                        }
                );

        neighborMonitor.scheduleAtFixedRate(
                () -> workerService.removeUnreachableNeighbors(),
                NEIGHBOR_CHECK_INTERVAL_SECONDS,
                NEIGHBOR_CHECK_INTERVAL_SECONDS,
                TimeUnit.SECONDS
        );

        return neighborMonitor;
    }

    private static void connectToExistingWorker(
            WorkerInfo workerInfo,
            WorkerServiceImpl workerService,
            BootstrapService bootstrapService
    ) {
        try {
            WorkerInfo existingWorker =
                    bootstrapService.getRandomWorker();

            if (existingWorker == null) {
                System.out.println(
                        "No existing workers found. "
                                + "Starting as the first worker."
                );
                return;
            }

            Registry existingRegistry =
                    LocateRegistry.getRegistry(
                            existingWorker.host(),
                            existingWorker.registryPort()
                    );

            WorkerService existingService =
                    (WorkerService) existingRegistry.lookup(
                            existingWorker.bindingName()
                    );

            if (!existingService.ping()) {
                System.err.println(
                        "Existing worker did not respond: "
                                + existingWorker.workerId()
                );
                return;
            }

            workerService.addNeighbor(existingWorker);
            existingService.addNeighbor(workerInfo);

            System.out.println(
                    "Connected to existing worker: "
                            + existingWorker.workerId()
            );
        } catch (Exception exception) {
            System.err.println(
                    "Could not connect to an existing worker: "
                            + exception.getMessage()
            );
        }
    }

    private static void notifyNeighborsOfShutdown(
            WorkerInfo workerInfo,
            WorkerServiceImpl workerService
    ) {
        List<WorkerInfo> neighbors;

        try {
            neighbors = workerService.getNeighbors();
        } catch (Exception exception) {
            System.err.println(
                    "Unable to retrieve neighbors during shutdown: "
                            + exception.getMessage()
            );
            return;
        }

        for (WorkerInfo neighbor : neighbors) {
            try {
                Registry registry =
                        LocateRegistry.getRegistry(
                                neighbor.host(),
                                neighbor.registryPort()
                        );

                WorkerService neighborService =
                        (WorkerService) registry.lookup(
                                neighbor.bindingName()
                        );

                neighborService.removeNeighbor(
                        workerInfo.workerId()
                );

                System.out.println(
                        "Notified neighbor of shutdown: "
                                + neighbor.workerId()
                );
            } catch (Exception exception) {
                System.err.println(
                        "Could not notify neighbor "
                                + neighbor.workerId()
                                + ": "
                                + exception.getMessage()
                );
            }
        }
    }

    private static int parseInteger(
            String value,
            String fieldName
    ) {
        try {
            int number = Integer.parseInt(value);

            if (number < 0) {
                throw new IllegalArgumentException(
                        fieldName + " cannot be negative"
                );
            }

            return number;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(
                    fieldName + " must be a number"
            );
        }
    }

    private static int parsePort(
            String value,
            String fieldName
    ) {
        int port = parseInteger(
                value,
                fieldName
        );

        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException(
                    fieldName
                            + " must be between 1 and 65535"
            );
        }

        return port;
    }

    private static void printUsage() {
        System.out.println(
                "Usage: WorkerServer "
                        + "<worker-id> <host> <worker-port> "
                        + "<bootstrap-host> <bootstrap-port>"
        );

        System.out.println(
                "Example: WorkerServer "
                        + "1 localhost 1101 localhost 1099"
        );
    }
}