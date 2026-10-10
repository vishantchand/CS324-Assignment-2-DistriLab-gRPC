package fj.ac.usp.cs324.distrilab.bootstrap;

import fj.ac.usp.cs324.distrilab.common.model.WorkerInfo;
import fj.ac.usp.cs324.distrilab.common.rmi.BootstrapService;
import fj.ac.usp.cs324.distrilab.common.rmi.WorkerService;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

public class BootstrapServiceImpl extends UnicastRemoteObject
        implements BootstrapService {

    private static final long serialVersionUID = 1L;

    private final ConcurrentHashMap<Integer, WorkerInfo>
            activeWorkers;

    public BootstrapServiceImpl() throws RemoteException {
        super();
        activeWorkers = new ConcurrentHashMap<>();
    }

    @Override
    public void registerWorker(WorkerInfo worker)
            throws RemoteException {
        if (worker == null) {
            throw new IllegalArgumentException(
                    "worker cannot be null"
            );
        }

        WorkerInfo existing = activeWorkers.putIfAbsent(
                worker.workerId(),
                worker
        );

        if (existing != null && !existing.equals(worker)) {
            throw new IllegalArgumentException(
                    "worker ID "
                            + worker.workerId()
                            + " is already registered"
            );
        }

        System.out.println(
                "Registered worker: " + worker
        );
    }

    @Override
    public void unregisterWorker(int workerId)
            throws RemoteException {
        WorkerInfo removedWorker =
                activeWorkers.remove(workerId);

        if (removedWorker != null) {
            System.out.println(
                    "Unregistered worker: "
                            + removedWorker
            );
        }
    }

    @Override
    public List<WorkerInfo> getActiveWorkers()
            throws RemoteException {
        return new ArrayList<>(
                activeWorkers.values()
        );
    }

    @Override
    public WorkerInfo getRandomWorker()
            throws RemoteException {
        List<WorkerInfo> workers =
                getActiveWorkers();

        if (workers.isEmpty()) {
            return null;
        }

        int randomIndex =
                ThreadLocalRandom.current()
                        .nextInt(workers.size());

        return workers.get(randomIndex);
    }

    public int removeUnreachableWorkers() {
        List<WorkerInfo> workerSnapshot =
                new ArrayList<>(
                        activeWorkers.values()
                );

        int removedCount = 0;

        for (WorkerInfo worker : workerSnapshot) {
            if (!isWorkerReachable(worker)) {
                boolean removed =
                        activeWorkers.remove(
                                worker.workerId(),
                                worker
                        );

                if (removed) {
                    removedCount++;

                    System.err.println(
                            "Removed unreachable worker "
                                    + "from Bootstrap Node: "
                                    + worker.workerId()
                    );
                }
            }
        }

        return removedCount;
    }

    private boolean isWorkerReachable(
            WorkerInfo worker
    ) {
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

            return workerService.ping();
        } catch (Exception exception) {
            return false;
        }
    }
}