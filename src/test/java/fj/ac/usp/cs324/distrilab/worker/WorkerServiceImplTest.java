package fj.ac.usp.cs324.distrilab.worker;

import fj.ac.usp.cs324.distrilab.common.model.JobRequest;
import fj.ac.usp.cs324.distrilab.common.model.JobResult;
import fj.ac.usp.cs324.distrilab.common.model.JobType;
import fj.ac.usp.cs324.distrilab.common.model.WorkerInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.rmi.NoSuchObjectException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerServiceImplTest {

    private WorkerInfo localWorker;
    private TrackingWorkerService workerService;

    private final List<Remote> exportedObjects =
            new ArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        localWorker = new WorkerInfo(
                1,
                "localhost",
                1101,
                "Worker-1"
        );

        workerService = new TrackingWorkerService(
                localWorker,
                0,
                false
        );

        exportedObjects.add(workerService);
    }

    @AfterEach
    void tearDown() {
        for (int index = exportedObjects.size() - 1;
             index >= 0;
             index--) {

            try {
                UnicastRemoteObject.unexportObject(
                        exportedObjects.get(index),
                        true
                );
            } catch (NoSuchObjectException ignored) {
                // The object was already removed.
            }
        }

        exportedObjects.clear();
    }

    @Test
    void returnsWorkerInformation() throws Exception {
        assertEquals(
                localWorker,
                workerService.getWorkerInfo()
        );
    }

    @Test
    void respondsToPing() throws Exception {
        assertTrue(workerService.ping());
    }

    @Test
    void addsAndListsNeighbor() throws Exception {
        WorkerInfo neighbor = new WorkerInfo(
                2,
                "localhost",
                1102,
                "Worker-2"
        );

        workerService.addNeighbor(neighbor);

        List<WorkerInfo> neighbors =
                workerService.getNeighbors();

        assertEquals(1, neighbors.size());
        assertTrue(neighbors.contains(neighbor));
    }

    @Test
    void ignoresItselfAsNeighbor() throws Exception {
        workerService.addNeighbor(localWorker);

        assertTrue(
                workerService.getNeighbors().isEmpty()
        );
    }

    @Test
    void rejectsNullNeighbor() {
        assertThrows(
                IllegalArgumentException.class,
                () -> workerService.addNeighbor(null)
        );
    }

    @Test
    void replacesNeighborWithSameWorkerId()
            throws Exception {
        WorkerInfo originalNeighbor = new WorkerInfo(
                2,
                "localhost",
                1102,
                "Worker-2"
        );

        WorkerInfo updatedNeighbor = new WorkerInfo(
                2,
                "localhost",
                1202,
                "Worker-2-Updated"
        );

        workerService.addNeighbor(originalNeighbor);
        workerService.addNeighbor(updatedNeighbor);

        List<WorkerInfo> neighbors =
                workerService.getNeighbors();

        assertEquals(1, neighbors.size());
        assertTrue(neighbors.contains(updatedNeighbor));
        assertFalse(neighbors.contains(originalNeighbor));
    }

    @Test
    void removesNeighbor() throws Exception {
        WorkerInfo neighbor = new WorkerInfo(
                2,
                "localhost",
                1102,
                "Worker-2"
        );

        workerService.addNeighbor(neighbor);
        workerService.removeNeighbor(
                neighbor.workerId()
        );

        assertTrue(
                workerService.getNeighbors().isEmpty()
        );
    }

    @Test
    void removesUnreachableNeighbor() throws Exception {
        int unavailablePort;

        try (ServerSocket socket = new ServerSocket(0)) {
            unavailablePort = socket.getLocalPort();
        }

        WorkerInfo unreachableNeighbor = new WorkerInfo(
                2,
                "localhost",
                unavailablePort,
                "Worker-2"
        );

        workerService.addNeighbor(unreachableNeighbor);

        assertEquals(
                1,
                workerService.getNeighbors().size()
        );

        workerService.removeUnreachableNeighbors();

        assertTrue(
                workerService.getNeighbors().isEmpty()
        );
    }

    @Test
    void distributesUnevenMaxJobAcrossThreeWorkers()
            throws Exception {
        TrackingWorkerService secondWorker =
                createRemoteWorker(2, false);

        TrackingWorkerService thirdWorker =
                createRemoteWorker(3, false);

        JobRequest request = new JobRequest(
                "distributed-max",
                JobType.MAX,
                List.of(
                        1L, 2L, 3L, 4L, 5L,
                        6L, 7L, 8L, 9L, 10L
                ),
                0L,
                0L
        );

        JobResult result =
                workerService.submitJob(request);

        assertTrue(result.successful());
        assertEquals(10L, result.value());

        assertEquals(
                1,
                workerService.getExecutionCount()
        );

        assertEquals(
                1,
                secondWorker.getExecutionCount()
        );

        assertEquals(
                1,
                thirdWorker.getExecutionCount()
        );

        List<Integer> partitionSizes =
                new ArrayList<>();

        partitionSizes.addAll(
                workerService.getReceivedPartitionSizes()
        );

        partitionSizes.addAll(
                secondWorker.getReceivedPartitionSizes()
        );

        partitionSizes.addAll(
                thirdWorker.getReceivedPartitionSizes()
        );

        partitionSizes.sort(Integer::compareTo);

        assertEquals(
                List.of(3, 3, 4),
                partitionSizes
        );
    }

    @Test
    void combinesPrimeSumResultsFromMultipleWorkers()
            throws Exception {
        TrackingWorkerService secondWorker =
                createRemoteWorker(2, false);

        TrackingWorkerService thirdWorker =
                createRemoteWorker(3, false);

        JobRequest request = new JobRequest(
                "distributed-prime-sum",
                JobType.PRIMESUM,
                List.of(),
                1L,
                10L
        );

        JobResult result =
                workerService.submitJob(request);

        assertTrue(result.successful());
        assertEquals(17L, result.value());

        assertEquals(
                1,
                workerService.getExecutionCount()
        );

        assertEquals(
                1,
                secondWorker.getExecutionCount()
        );

        assertEquals(
                1,
                thirdWorker.getExecutionCount()
        );
    }

    @Test
    void combinesPrimeCountResultsFromMultipleWorkers()
            throws Exception {
        TrackingWorkerService secondWorker =
                createRemoteWorker(2, false);

        TrackingWorkerService thirdWorker =
                createRemoteWorker(3, false);

        JobRequest request = new JobRequest(
                "distributed-prime-count",
                JobType.PRIMECOUNT,
                List.of(
                        1L, 2L, 3L, 4L, 5L,
                        6L, 7L, 8L, 9L, 10L
                ),
                0L,
                0L
        );

        JobResult result =
                workerService.submitJob(request);

        assertTrue(result.successful());
        assertEquals(4L, result.value());

        assertEquals(
                1,
                workerService.getExecutionCount()
        );

        assertEquals(
                1,
                secondWorker.getExecutionCount()
        );

        assertEquals(
                1,
                thirdWorker.getExecutionCount()
        );
    }

    @Test
    void retriesPartitionWhenWorkerFails()
            throws Exception {
        TrackingWorkerService failingWorker =
                createRemoteWorker(2, true);

        TrackingWorkerService healthyWorker =
                createRemoteWorker(3, false);

        JobRequest request = new JobRequest(
                "failure-recovery",
                JobType.MAX,
                List.of(
                        5L, 10L, 15L,
                        20L, 25L, 30L
                ),
                0L,
                0L
        );

        JobResult result =
                workerService.submitJob(request);

        assertTrue(result.successful());
        assertEquals(30L, result.value());

        assertEquals(
                1,
                failingWorker.getExecutionCount()
        );

        assertEquals(
                1,
                healthyWorker.getExecutionCount()
        );

        assertEquals(
                2,
                workerService.getExecutionCount()
        );

        assertFalse(
                workerService.getNeighbors().contains(
                        failingWorker.getWorkerInfo()
                )
        );
    }

    @Test
    void incrementsJacAfterDistributedAssignment()
            throws Exception {
        createRemoteWorker(2, false);
        createRemoteWorker(3, false);

        int jacBeforeAssignment =
                workerService.getJac();

        JobRequest request = new JobRequest(
                "jac-test",
                JobType.MAX,
                List.of(2L, 4L, 6L, 8L),
                0L,
                0L
        );

        JobResult result =
                workerService.submitJob(request);

        assertTrue(result.successful());

        assertEquals(
                jacBeforeAssignment + 1,
                workerService.getJac()
        );
    }

    private TrackingWorkerService createRemoteWorker(
            int workerId,
            boolean failDuringExecution
    ) throws Exception {
        int port = findAvailablePort();

        String bindingName =
                "Worker-" + workerId;

        WorkerInfo workerInfo = new WorkerInfo(
                workerId,
                "localhost",
                port,
                bindingName
        );

        Registry registry =
                LocateRegistry.createRegistry(port);

        exportedObjects.add(registry);

        TrackingWorkerService service =
                new TrackingWorkerService(
                        workerInfo,
                        10,
                        failDuringExecution
                );

        exportedObjects.add(service);

        registry.rebind(
                bindingName,
                service
        );

        workerService.addNeighbor(workerInfo);

        return service;
    }

    private int findAvailablePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static class TrackingWorkerService
            extends WorkerServiceImpl {

        private final AtomicInteger executionCount;
        private final List<Integer> receivedPartitionSizes;
        private final boolean failDuringExecution;

        TrackingWorkerService(
                WorkerInfo workerInfo,
                int initialJac,
                boolean failDuringExecution
        ) throws RemoteException {
            super(workerInfo, initialJac);

            this.executionCount = new AtomicInteger();
            this.receivedPartitionSizes =
                    new CopyOnWriteArrayList<>();

            this.failDuringExecution =
                    failDuringExecution;
        }

        @Override
        public JobResult executeJob(JobRequest request)
                throws RemoteException {
            executionCount.incrementAndGet();

            receivedPartitionSizes.add(
                    request.numbers().size()
            );

            if (failDuringExecution) {
                throw new RemoteException(
                        "Simulated worker failure"
                );
            }

            return super.executeJob(request);
        }

        int getExecutionCount() {
            return executionCount.get();
        }

        List<Integer> getReceivedPartitionSizes() {
            return List.copyOf(
                    receivedPartitionSizes
            );
        }
    }
}