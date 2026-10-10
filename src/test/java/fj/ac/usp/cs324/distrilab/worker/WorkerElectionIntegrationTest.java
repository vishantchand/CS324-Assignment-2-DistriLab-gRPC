package fj.ac.usp.cs324.distrilab.worker;

import fj.ac.usp.cs324.distrilab.common.model.JobRequest;
import fj.ac.usp.cs324.distrilab.common.model.JobType;
import fj.ac.usp.cs324.distrilab.common.model.WorkerInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class WorkerElectionIntegrationTest {

    private final List<WorkerServiceImpl> services =
            new ArrayList<>();

    private final List<Registry> registries =
            new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (WorkerServiceImpl service : services) {
            try {
                UnicastRemoteObject.unexportObject(
                        service,
                        true
                );
            } catch (Exception ignored) {
            }
        }

        for (Registry registry : registries) {
            try {
                UnicastRemoteObject.unexportObject(
                        registry,
                        true
                );
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    void propagatesElectionAndAllWorkersAgreeOnLowestJac()
            throws Exception {
        WorkerNode workerOne = createWorker(1, 4);
        WorkerNode workerTwo = createWorker(2, 1);
        WorkerNode workerThree = createWorker(3, 3);

        connectBidirectionally(workerOne, workerTwo);
        connectBidirectionally(workerTwo, workerThree);

        workerOne.service().startElection();

        assertEquals(
                2,
                workerOne.service().getCoordinatorId()
        );

        assertEquals(
                2,
                workerTwo.service().getCoordinatorId()
        );

        assertEquals(
                2,
                workerThree.service().getCoordinatorId()
        );
    }

    @Test
    void cyclicNetworkPreventsDuplicateLoopAndUsesHighestId()
            throws Exception {
        WorkerNode workerOne = createWorker(1, 2);
        WorkerNode workerTwo = createWorker(2, 2);
        WorkerNode workerThree = createWorker(3, 2);

        connectBidirectionally(workerOne, workerTwo);
        connectBidirectionally(workerTwo, workerThree);
        connectBidirectionally(workerThree, workerOne);

        workerOne.service().startElection();

        assertEquals(
                3,
                workerOne.service().getCoordinatorId()
        );

        assertEquals(
                3,
                workerTwo.service().getCoordinatorId()
        );

        assertEquals(
                3,
                workerThree.service().getCoordinatorId()
        );
    }

    @Test
    void continuesElectionWhenNeighborIsUnreachable()
            throws Exception {
        WorkerNode workerOne = createWorker(1, 0);

        int unavailablePort = findFreePort();

        WorkerInfo unreachableWorker = new WorkerInfo(
                99,
                "localhost",
                unavailablePort,
                "Worker-99"
        );

        workerOne.service().addNeighbor(
                unreachableWorker
        );

        workerOne.service().startElection();

        assertEquals(
                1,
                workerOne.service().getCoordinatorId()
        );

        assertFalse(
                workerOne.service()
                        .getNeighbors()
                        .contains(unreachableWorker)
        );
    }

    @Test
    void startsNewElectionAutomaticallyAfterFiveJobs()
            throws Exception {
        WorkerNode workerOne = createWorker(1, 0);
        WorkerNode workerTwo = createWorker(2, 10);

        connectBidirectionally(workerOne, workerTwo);

        workerOne.service().startElection();

        assertEquals(
                1,
                workerOne.service().getCoordinatorId()
        );

        for (int jobNumber = 1;
             jobNumber <= 5;
             jobNumber++) {

            JobRequest request = new JobRequest(
                    "job-" + jobNumber,
                    JobType.MAX,
                    List.of(
                            (long) jobNumber,
                            20L,
                            10L
                    ),
                    0L,
                    0L
            );

            workerOne.service().submitJob(request);
        }

        assertEquals(5, workerOne.service().getJac());

        assertEquals(
                0,
                workerOne.service().getJobsInCurrentTerm()
        );

        assertEquals(
                1,
                workerOne.service().getCoordinatorId()
        );

        assertEquals(
                1,
                workerTwo.service().getCoordinatorId()
        );
    }

    private WorkerNode createWorker(
            int workerId,
            int initialJac
    ) throws Exception {
        int registryPort = findFreePort();
        String bindingName = "Worker-" + workerId;

        WorkerInfo workerInfo = new WorkerInfo(
                workerId,
                "localhost",
                registryPort,
                bindingName
        );

        Registry registry =
                LocateRegistry.createRegistry(
                        registryPort
                );

        WorkerServiceImpl service =
                new WorkerServiceImpl(
                        workerInfo,
                        initialJac
                );

        registry.rebind(bindingName, service);

        registries.add(registry);
        services.add(service);

        return new WorkerNode(workerInfo, service);
    }

    private void connectBidirectionally(
            WorkerNode first,
            WorkerNode second
    ) throws Exception {
        first.service().addNeighbor(second.info());
        second.service().addNeighbor(first.info());
    }

    private int findFreePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private record WorkerNode(
            WorkerInfo info,
            WorkerServiceImpl service
    ) {
    }
}