package fj.ac.usp.cs324.distrilab.bootstrap;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class BootstrapServer {

    public static final int DEFAULT_PORT = 1099;

    public static final String BINDING_NAME =
            "BootstrapService";

    private static final long
            WORKER_HEALTH_CHECK_INTERVAL_SECONDS = 5;

    private BootstrapServer() {
    }

    public static void main(String[] args) {
        int port;

        try {
            port = parsePort(args);
        } catch (IllegalArgumentException exception) {
            System.err.println(
                    "Unable to start Bootstrap Node: "
                            + exception.getMessage()
            );

            System.err.println(
                    "Usage: BootstrapServer [registry-port]"
            );

            return;
        }

        try {
            Registry registry =
                    LocateRegistry.createRegistry(port);

            BootstrapServiceImpl service =
                    new BootstrapServiceImpl();

            registry.rebind(
                    BINDING_NAME,
                    service
            );

            ScheduledExecutorService healthMonitor =
                    createHealthMonitor(service);

            Runtime.getRuntime().addShutdownHook(
                    new Thread(
                            () -> healthMonitor.shutdownNow(),
                            "bootstrap-shutdown"
                    )
            );

            System.out.println(
                    "Bootstrap Node started successfully."
            );

            System.out.println(
                    "Registry port: " + port
            );

            System.out.println(
                    "Binding name: " + BINDING_NAME
            );

            System.out.println(
                    "Worker health-check interval: "
                            + WORKER_HEALTH_CHECK_INTERVAL_SECONDS
                            + " seconds"
            );

            System.out.println(
                    "Waiting for worker registrations..."
            );

            new CountDownLatch(1).await();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();

            System.err.println(
                    "Bootstrap Node was interrupted."
            );
        } catch (Exception exception) {
            System.err.println(
                    "Unable to start Bootstrap Node: "
                            + exception.getMessage()
            );

            exception.printStackTrace();
        }
    }

    private static ScheduledExecutorService
    createHealthMonitor(
            BootstrapServiceImpl service
    ) {
        ScheduledExecutorService monitor =
                Executors.newSingleThreadScheduledExecutor(
                        runnable -> {
                            Thread thread = new Thread(
                                    runnable,
                                    "bootstrap-worker-health-monitor"
                            );

                            thread.setDaemon(true);
                            return thread;
                        }
                );

        monitor.scheduleAtFixedRate(
                service::removeUnreachableWorkers,
                WORKER_HEALTH_CHECK_INTERVAL_SECONDS,
                WORKER_HEALTH_CHECK_INTERVAL_SECONDS,
                TimeUnit.SECONDS
        );

        return monitor;
    }

    private static int parsePort(String[] args) {
        if (args.length == 0) {
            return DEFAULT_PORT;
        }

        if (args.length > 1) {
            throw new IllegalArgumentException(
                    "only one registry port may be supplied"
            );
        }

        try {
            int port =
                    Integer.parseInt(args[0]);

            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException(
                        "port must be between 1 and 65535"
                );
            }

            return port;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(
                    "registry port must be a number"
            );
        }
    }
}