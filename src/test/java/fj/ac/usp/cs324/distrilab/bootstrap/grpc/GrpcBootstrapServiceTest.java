package fj.ac.usp.cs324.distrilab.bootstrap.grpc;

import fj.ac.usp.cs324.distrilab.grpc.Acknowledgement;
import fj.ac.usp.cs324.distrilab.grpc.Empty;
import fj.ac.usp.cs324.distrilab.grpc.RegisterWorkerReply;
import fj.ac.usp.cs324.distrilab.grpc.WorkerIdRequest;
import fj.ac.usp.cs324.distrilab.grpc.WorkerInfo;
import fj.ac.usp.cs324.distrilab.grpc.WorkerList;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GrpcBootstrapServiceTest {

    private GrpcBootstrapService service;

    @BeforeEach
    void setUp() {
        service = new GrpcBootstrapService();
    }

    @Test
    void registersWorker() {
        RecordingObserver<RegisterWorkerReply> observer =
                new RecordingObserver<>();

        service.registerWorker(
                worker(1, 5001),
                observer
        );

        assertFalse(observer.hasError());
        assertTrue(observer.completed);
        assertTrue(observer.value.getSuccess());
        assertEquals(1, observer.value.getActiveWorkersCount());
        assertTrue(service.containsWorker(1));
    }

    @Test
    void identicalRegistrationIsIdempotent() {
        WorkerInfo worker = worker(1, 5001);

        service.registerWorker(
                worker,
                new RecordingObserver<>()
        );

        RecordingObserver<RegisterWorkerReply> observer =
                new RecordingObserver<>();

        service.registerWorker(worker, observer);

        assertFalse(observer.hasError());
        assertTrue(observer.value.getSuccess());
        assertEquals(1, service.getActiveWorkerCount());
    }

    @Test
    void rejectsConflictingWorkerId() {
        service.registerWorker(
                worker(1, 5001),
                new RecordingObserver<>()
        );

        RecordingObserver<RegisterWorkerReply> observer =
                new RecordingObserver<>();

        service.registerWorker(
                worker(1, 6001),
                observer
        );

        assertEquals(
                Status.Code.ALREADY_EXISTS,
                observer.statusCode()
        );
        assertEquals(1, service.getActiveWorkerCount());
    }

    @Test
    void rejectsInvalidWorker() {
        WorkerInfo invalidWorker =
                WorkerInfo.newBuilder()
                        .setWorkerId(1)
                        .setHost("")
                        .setGrpcPort(5001)
                        .setWorkerName("Worker-1")
                        .build();

        RecordingObserver<RegisterWorkerReply> observer =
                new RecordingObserver<>();

        service.registerWorker(
                invalidWorker,
                observer
        );

        assertEquals(
                Status.Code.INVALID_ARGUMENT,
                observer.statusCode()
        );
        assertEquals(0, service.getActiveWorkerCount());
    }

    @Test
    void unregistersWorker() {
        service.registerWorker(
                worker(1, 5001),
                new RecordingObserver<>()
        );

        RecordingObserver<Acknowledgement> observer =
                new RecordingObserver<>();

        service.unregisterWorker(
                WorkerIdRequest.newBuilder()
                        .setWorkerId(1)
                        .build(),
                observer
        );

        assertFalse(observer.hasError());
        assertTrue(observer.value.getSuccess());
        assertFalse(service.containsWorker(1));
    }

    @Test
    void listsWorkersInIdOrder() {
        service.registerWorker(
                worker(3, 5003),
                new RecordingObserver<>()
        );
        service.registerWorker(
                worker(1, 5001),
                new RecordingObserver<>()
        );
        service.registerWorker(
                worker(2, 5002),
                new RecordingObserver<>()
        );

        RecordingObserver<WorkerList> observer =
                new RecordingObserver<>();

        service.listActiveWorkers(
                Empty.getDefaultInstance(),
                observer
        );

        List<WorkerInfo> workers =
                observer.value.getWorkersList();

        assertEquals(3, workers.size());
        assertEquals(1, workers.get(0).getWorkerId());
        assertEquals(2, workers.get(1).getWorkerId());
        assertEquals(3, workers.get(2).getWorkerId());
    }

    @Test
    void returnsRegisteredRandomWorker() {
        service.registerWorker(
                worker(1, 5001),
                new RecordingObserver<>()
        );
        service.registerWorker(
                worker(2, 5002),
                new RecordingObserver<>()
        );

        RecordingObserver<WorkerInfo> observer =
                new RecordingObserver<>();

        service.getRandomWorker(
                Empty.getDefaultInstance(),
                observer
        );

        assertFalse(observer.hasError());
        assertNotNull(observer.value);
        assertTrue(
                observer.value.getWorkerId() == 1
                        || observer.value.getWorkerId() == 2
        );
    }

    @Test
    void returnsNotFoundWhenNoWorkersExist() {
        RecordingObserver<WorkerInfo> observer =
                new RecordingObserver<>();

        service.getRandomWorker(
                Empty.getDefaultInstance(),
                observer
        );

        assertEquals(
                Status.Code.NOT_FOUND,
                observer.statusCode()
        );
    }

    @Test
    void conditionalRemovalProtectsUpdatedRegistration() {
        WorkerInfo original = worker(1, 5001);

        service.registerWorker(
                original,
                new RecordingObserver<>()
        );

        assertFalse(
                service.removeWorkerIfUnchanged(
                        1,
                        worker(1, 6001)
                )
        );

        assertTrue(service.containsWorker(1));

        assertTrue(
                service.removeWorkerIfUnchanged(
                        1,
                        original
                )
        );

        assertFalse(service.containsWorker(1));
    }

    private static WorkerInfo worker(
            int workerId,
            int port
    ) {
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
            this.completed = true;
        }

        private boolean hasError() {
            return error != null;
        }

        private Status.Code statusCode() {
            return Status.fromThrowable(error).getCode();
        }
    }
}