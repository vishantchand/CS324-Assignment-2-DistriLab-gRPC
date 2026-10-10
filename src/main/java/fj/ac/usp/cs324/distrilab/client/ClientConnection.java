package fj.ac.usp.cs324.distrilab.client;

import fj.ac.usp.cs324.distrilab.common.model.JobRequest;
import fj.ac.usp.cs324.distrilab.common.model.JobResult;
import fj.ac.usp.cs324.distrilab.common.model.WorkerInfo;
import fj.ac.usp.cs324.distrilab.common.rmi.BootstrapService;
import fj.ac.usp.cs324.distrilab.common.rmi.WorkerService;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class ClientConnection {

    private static final String BOOTSTRAP_BINDING =
            "BootstrapService";

    private final String bootstrapHost;
    private final int bootstrapPort;

    public ClientConnection(
            String bootstrapHost,
            int bootstrapPort
    ) {
        if (bootstrapHost == null
                || bootstrapHost.isBlank()) {
            throw new IllegalArgumentException(
                    "Bootstrap host cannot be blank"
            );
        }

        if (bootstrapPort < 1 || bootstrapPort > 65535) {
            throw new IllegalArgumentException(
                    "Bootstrap port must be between 1 and 65535"
            );
        }

        this.bootstrapHost = bootstrapHost.trim();
        this.bootstrapPort = bootstrapPort;
    }

    public JobResult submitJob(JobRequest request)
            throws RemoteException {
        Objects.requireNonNull(
                request,
                "Job request cannot be null"
        );

        WorkerService workerService =
                findReachableWorker();

        return workerService.submitJob(request);
    }

    public WorkerInfo findReachableWorkerInfo()
            throws RemoteException {
        return findReachableWorkerConnection().workerInfo();
    }

    private WorkerService findReachableWorker()
            throws RemoteException {
        return findReachableWorkerConnection().workerService();
    }

    private WorkerConnection findReachableWorkerConnection()
            throws RemoteException {
        BootstrapService bootstrapService =
                lookupBootstrapService();

        Map<Integer, WorkerInfo> candidates =
                new LinkedHashMap<>();

        try {
            WorkerInfo randomWorker =
                    bootstrapService.getRandomWorker();

            if (randomWorker != null) {
                candidates.put(
                        randomWorker.workerId(),
                        randomWorker
                );
            }
        } catch (Exception exception) {
            System.err.println(
                    "Random-worker discovery failed: "
                            + exception.getMessage()
            );
        }

        List<WorkerInfo> activeWorkers =
                bootstrapService.getActiveWorkers();

        for (WorkerInfo worker : activeWorkers) {
            candidates.putIfAbsent(
                    worker.workerId(),
                    worker
            );
        }

        if (candidates.isEmpty()) {
            throw new RemoteException(
                    "No active workers are registered"
            );
        }

        Exception lastFailure = null;

        for (WorkerInfo worker : candidates.values()) {
            try {
                Registry workerRegistry =
                        LocateRegistry.getRegistry(
                                worker.host(),
                                worker.registryPort()
                        );

                WorkerService workerService =
                        (WorkerService) workerRegistry.lookup(
                                worker.bindingName()
                        );

                if (workerService.ping()) {
                    return new WorkerConnection(
                            worker,
                            workerService
                    );
                }
            } catch (Exception exception) {
                lastFailure = exception;

                System.err.println(
                        "Worker "
                                + worker.workerId()
                                + " is unavailable: "
                                + exception.getMessage()
                );
            }
        }

        RemoteException exception = new RemoteException(
                "No registered worker is reachable"
        );

        if (lastFailure != null) {
            exception.initCause(lastFailure);
        }

        throw exception;
    }

    private BootstrapService lookupBootstrapService()
            throws RemoteException {
        try {
            Registry registry =
                    LocateRegistry.getRegistry(
                            bootstrapHost,
                            bootstrapPort
                    );

            return (BootstrapService) registry.lookup(
                    BOOTSTRAP_BINDING
            );
        } catch (Exception exception) {
            throw new RemoteException(
                    "Unable to connect to Bootstrap Node at "
                            + bootstrapHost
                            + ":"
                            + bootstrapPort,
                    exception
            );
        }
    }

    private record WorkerConnection(
            WorkerInfo workerInfo,
            WorkerService workerService
    ) {
    }
}