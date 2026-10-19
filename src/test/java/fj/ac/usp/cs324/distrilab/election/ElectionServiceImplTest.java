package fj.ac.usp.cs324.distrilab.election;

import fj.ac.usp.cs324.distrilab.common.model.CoordinatorMessage;
import fj.ac.usp.cs324.distrilab.grpc.Acknowledgement;
import fj.ac.usp.cs324.distrilab.grpc.CoordinatorAnnouncement;
import fj.ac.usp.cs324.distrilab.grpc.ElectionReply;
import fj.ac.usp.cs324.distrilab.grpc.ElectionRequest;
import fj.ac.usp.cs324.distrilab.grpc.HeartbeatReply;
import fj.ac.usp.cs324.distrilab.grpc.HeartbeatRequest;
import fj.ac.usp.cs324.distrilab.grpc.WorkerHealth;
import fj.ac.usp.cs324.distrilab.grpc.WorkerInfo;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ElectionServiceImplTest {

    @Test
    void receiveElectionSelectsWorkerWithLowerJac() {
        ElectionManager manager = new ElectionManager(2, 3);
        ElectionServiceImpl service = createService(manager);

        ElectionRequest request = ElectionRequest.newBuilder()
                .setElectionId("election-1")
                .setInitiatorId(1)
                .setCandidateId(1)
                .setCandidateJac(5)
                .setProposedTerm(1)
                .build();

        TestObserver<ElectionReply> observer =
                new TestObserver<>();

        service.receiveElection(request, observer);

        assertNull(observer.error);
        assertTrue(observer.completed);
        assertNotNull(observer.value);
        assertTrue(observer.value.getAccepted());
        assertEquals(2, observer.value.getCandidateId());
        assertEquals(3, observer.value.getCandidateJac());
        assertEquals(1, observer.value.getProposedTerm());
    }

    @Test
    void receiveElectionRejectsStaleTerm() {
        ElectionManager manager = new ElectionManager(2, 3);

        manager.receiveCoordinator(
                new CoordinatorMessage(
                        "existing-election",
                        2,
                        3
                ),
                2
        );

        ElectionServiceImpl service = createService(manager);

        ElectionRequest request = ElectionRequest.newBuilder()
                .setElectionId("stale-election")
                .setInitiatorId(1)
                .setCandidateId(1)
                .setCandidateJac(5)
                .setProposedTerm(1)
                .build();

        TestObserver<ElectionReply> observer =
                new TestObserver<>();

        service.receiveElection(request, observer);

        assertNull(observer.error);
        assertTrue(observer.completed);
        assertNotNull(observer.value);
        assertFalse(observer.value.getAccepted());
        assertEquals(
                "Election term is stale",
                observer.value.getMessage()
        );
        assertEquals(2, observer.value.getProposedTerm());
    }

    @Test
    void announceCoordinatorStoresCoordinatorAndTerm() {
        ElectionManager manager = new ElectionManager(2, 3);
        ElectionServiceImpl service = createService(manager);

        CoordinatorAnnouncement request =
                CoordinatorAnnouncement.newBuilder()
                        .setElectionId("election-2")
                        .setCoordinatorId(4)
                        .setCoordinatorJac(1)
                        .setCoordinatorTerm(1)
                        .setAnnouncedAtMs(
                                System.currentTimeMillis()
                        )
                        .build();

        TestObserver<Acknowledgement> observer =
                new TestObserver<>();

        service.announceCoordinator(request, observer);

        assertNull(observer.error);
        assertTrue(observer.completed);
        assertNotNull(observer.value);
        assertTrue(observer.value.getSuccess());
        assertEquals(4, manager.getCoordinatorId());
        assertEquals(1, manager.getCoordinatorJac());
        assertEquals(1, manager.getCoordinatorTerm());
    }

    @Test
    void coordinatorHeartbeatAcceptsMatchingCoordinator() {
        ElectionManager manager = new ElectionManager(2, 3);

        manager.receiveCoordinator(
                new CoordinatorMessage(
                        "election-3",
                        2,
                        3
                ),
                1
        );

        ElectionServiceImpl service = createService(manager);

        HeartbeatRequest request = HeartbeatRequest.newBuilder()
                .setSenderId(8)
                .setKnownCoordinatorId(2)
                .setCoordinatorTerm(1)
                .setTimestampMs(System.currentTimeMillis())
                .build();

        TestObserver<HeartbeatReply> observer =
                new TestObserver<>();

        service.coordinatorHeartbeat(request, observer);

        assertNull(observer.error);
        assertTrue(observer.completed);
        assertNotNull(observer.value);
        assertTrue(observer.value.getAlive());
        assertEquals(
                WorkerHealth.WORKER_HEALTH_ACTIVE,
                observer.value.getHealth()
        );
        assertEquals(2, observer.value.getWorker().getWorkerId());
        assertEquals(2, observer.value.getCoordinatorId());
        assertEquals(1, observer.value.getCoordinatorTerm());
    }

    @Test
    void coordinatorHeartbeatRejectsIncorrectTerm() {
        ElectionManager manager = new ElectionManager(2, 3);

        manager.receiveCoordinator(
                new CoordinatorMessage(
                        "election-4",
                        2,
                        3
                ),
                1
        );

        ElectionServiceImpl service = createService(manager);

        HeartbeatRequest request = HeartbeatRequest.newBuilder()
                .setSenderId(8)
                .setKnownCoordinatorId(2)
                .setCoordinatorTerm(2)
                .setTimestampMs(System.currentTimeMillis())
                .build();

        TestObserver<HeartbeatReply> observer =
                new TestObserver<>();

        service.coordinatorHeartbeat(request, observer);

        assertNull(observer.error);
        assertTrue(observer.completed);
        assertNotNull(observer.value);
        assertFalse(observer.value.getAlive());
        assertEquals(
                WorkerHealth.WORKER_HEALTH_SUSPECTED,
                observer.value.getHealth()
        );
        assertEquals(1, observer.value.getCoordinatorTerm());
    }

    private ElectionServiceImpl createService(
            ElectionManager manager
    ) {
        WorkerInfo workerInfo = WorkerInfo.newBuilder()
                .setWorkerId(manager.getWorkerId())
                .setHost("localhost")
                .setGrpcPort(50052)
                .setWorkerName(
                        "Worker-" + manager.getWorkerId()
                )
                .build();

        return new ElectionServiceImpl(
                manager,
                workerInfo
        );
    }

    private static final class TestObserver<T>
            implements StreamObserver<T> {

        private T value;
        private Throwable error;
        private boolean completed;

        @Override
        public void onNext(T value) {
            this.value = value;
        }

        @Override
        public void onError(Throwable error) {
            this.error = error;
        }

        @Override
        public void onCompleted() {
            this.completed = true;
        }
    }
}