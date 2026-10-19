package fj.ac.usp.cs324.distrilab.worker.grpc;

import fj.ac.usp.cs324.distrilab.grpc.Acknowledgement;
import fj.ac.usp.cs324.distrilab.grpc.Empty;
import fj.ac.usp.cs324.distrilab.grpc.HeartbeatReply;
import fj.ac.usp.cs324.distrilab.grpc.HeartbeatRequest;
import fj.ac.usp.cs324.distrilab.grpc.JoinNetworkReply;
import fj.ac.usp.cs324.distrilab.grpc.JoinNetworkRequest;
import fj.ac.usp.cs324.distrilab.grpc.WorkerFailureReport;
import fj.ac.usp.cs324.distrilab.grpc.WorkerHealth;
import fj.ac.usp.cs324.distrilab.grpc.WorkerIdRequest;
import fj.ac.usp.cs324.distrilab.grpc.WorkerInfo;
import fj.ac.usp.cs324.distrilab.grpc.WorkerList;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GrpcWorkerServiceTest {

    private WorkerMembershipManager membershipManager;
    private GrpcWorkerService service;
    private WorkerInfo localWorker;

    @BeforeEach
    void setUp() {
        localWorker = worker(1, 5001);
        membershipManager = new WorkerMembershipManager(1);

        service = new GrpcWorkerService(
                localWorker,
                membershipManager,
                () -> 3,
                () -> 7L
        );
    }

    @Test
    void returnsLocalWorkerInformation() {
        RecordingObserver<WorkerInfo> observer =
                new RecordingObserver<>();

        service.getWorkerInfo(
                Empty.getDefaultInstance(),
                observer
        );

        assertEquals(localWorker, observer.value);
        assertTrue(observer.completed);
        assertFalse(observer.hasError());
    }

    @Test
    void repliesToHeartbeatWithCoordinatorInformation() {
        RecordingObserver<HeartbeatReply> observer =
                new RecordingObserver<>();

        service.ping(
                HeartbeatRequest.newBuilder()
                        .setSenderId(2)
                        .setKnownCoordinatorId(3)
                        .setCoordinatorTerm(7)
                        .build(),
                observer
        );

        assertTrue(observer.value.getAlive());
        assertEquals(localWorker, observer.value.getWorker());
        assertEquals(
                WorkerHealth.WORKER_HEALTH_ACTIVE,
                observer.value.getHealth()
        );
        assertEquals(3, observer.value.getCoordinatorId());
        assertEquals(7L, observer.value.getCoordinatorTerm());
        assertTrue(observer.completed);
    }

    @Test
    void addsAndListsNeighbor() {
        RecordingObserver<Acknowledgement> addObserver =
                new RecordingObserver<>();

        service.addNeighbor(worker(2, 5002), addObserver);

        assertTrue(addObserver.value.getSuccess());
        assertTrue(membershipManager.containsNeighbour(2));

        RecordingObserver<WorkerList> listObserver =
                new RecordingObserver<>();

        service.listNeighbors(
                Empty.getDefaultInstance(),
                listObserver
        );

        assertEquals(1, listObserver.value.getWorkersCount());
        assertEquals(
                2,
                listObserver.value.getWorkers(0).getWorkerId()
        );
    }

    @Test
    void removesNeighbor() {
        membershipManager.addNeighbour(worker(2, 5002));

        RecordingObserver<Acknowledgement> observer =
                new RecordingObserver<>();

        service.removeNeighbor(
                WorkerIdRequest.newBuilder()
                        .setWorkerId(2)
                        .build(),
                observer
        );

        assertTrue(observer.value.getSuccess());
        assertFalse(membershipManager.containsNeighbour(2));
    }

    @Test
    void joinsWorkerAndReturnsAvailableNeighbors() {
        membershipManager.addNeighbour(worker(3, 5003));

        RecordingObserver<JoinNetworkReply> observer =
                new RecordingObserver<>();

        service.joinNetwork(
                JoinNetworkRequest.newBuilder()
                        .setWorker(worker(2, 5002))
                        .build(),
                observer
        );

        assertTrue(observer.value.getSuccess());
        assertTrue(membershipManager.containsNeighbour(2));
        assertEquals(2, observer.value.getNeighborsCount());
        assertEquals(
                1,
                observer.value.getNeighbors(0).getWorkerId()
        );
        assertEquals(
                3,
                observer.value.getNeighbors(1).getWorkerId()
        );
        assertEquals(3, observer.value.getCoordinatorId());
        assertEquals(7L, observer.value.getCoordinatorTerm());
    }

    @Test
    void removesWorkerAfterFailureReport() {
        membershipManager.addNeighbour(worker(2, 5002));

        RecordingObserver<Acknowledgement> observer =
                new RecordingObserver<>();

        service.reportWorkerFailure(
                WorkerFailureReport.newBuilder()
                        .setReporterId(1)
                        .setFailedWorkerId(2)
                        .setDetectedAtMs(
                                System.currentTimeMillis()
                        )
                        .setReason("Heartbeat deadline exceeded")
                        .build(),
                observer
        );

        assertTrue(observer.value.getSuccess());
        assertFalse(membershipManager.containsNeighbour(2));
    }

    @Test
    void rejectsInvalidWorkerInformation() {
        WorkerInfo invalidWorker = WorkerInfo.newBuilder()
                .setWorkerId(2)
                .setHost("")
                .setGrpcPort(5002)
                .setWorkerName("Worker-2")
                .build();

        RecordingObserver<Acknowledgement> observer =
                new RecordingObserver<>();

        service.addNeighbor(invalidWorker, observer);

        assertTrue(observer.hasError());
        assertInstanceOf(
                StatusRuntimeException.class,
                observer.error
        );

        StatusRuntimeException exception =
                (StatusRuntimeException) observer.error;

        assertEquals(
                Status.Code.INVALID_ARGUMENT,
                exception.getStatus().getCode()
        );
    }

    private static WorkerInfo worker(int workerId, int port) {
        return WorkerInfo.newBuilder()
                .setWorkerId(workerId)
                .setHost("localhost")
                .setGrpcPort(port)
                .setWorkerName("Worker-" + workerId)
                .build();
    }

    private static final class RecordingObserver<T>
            implements StreamObserver<T> {

        private T value;
        private Throwable error;
        private boolean completed;

        @Override
        public void onNext(T value) {
            this.value = value;
        }

        @Override
        public void onError(Throwable throwable) {
            this.error = throwable;
        }

        @Override
        public void onCompleted() {
            completed = true;
        }

        private boolean hasError() {
            return error != null;
        }
    }
}