package fj.ac.usp.cs324.distrilab.bootstrap.grpc;

import fj.ac.usp.cs324.distrilab.grpc.Acknowledgement;
import fj.ac.usp.cs324.distrilab.grpc.BootstrapServiceGrpc;
import fj.ac.usp.cs324.distrilab.grpc.Empty;
import fj.ac.usp.cs324.distrilab.grpc.RegisterWorkerReply;
import fj.ac.usp.cs324.distrilab.grpc.WorkerIdRequest;
import fj.ac.usp.cs324.distrilab.grpc.WorkerInfo;
import fj.ac.usp.cs324.distrilab.grpc.WorkerList;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

public final class GrpcBootstrapService
        extends BootstrapServiceGrpc.BootstrapServiceImplBase {

    private final ConcurrentHashMap<Integer, WorkerInfo>
            activeWorkers = new ConcurrentHashMap<>();

    @Override
    public void registerWorker(
            WorkerInfo worker,
            StreamObserver<RegisterWorkerReply> responseObserver
    ) {
        if (!isValidWorker(worker)) {
            responseObserver.onError(
                    Status.INVALID_ARGUMENT
                            .withDescription(
                                    "Worker ID, host, port and name are required"
                            )
                            .asRuntimeException()
            );
            return;
        }

        WorkerInfo existing = activeWorkers.putIfAbsent(
                worker.getWorkerId(),
                worker
        );

        if (existing != null && !existing.equals(worker)) {
            responseObserver.onError(
                    Status.ALREADY_EXISTS
                            .withDescription(
                                    "Worker ID "
                                            + worker.getWorkerId()
                                            + " is already registered"
                            )
                            .asRuntimeException()
            );
            return;
        }

        RegisterWorkerReply reply =
                RegisterWorkerReply.newBuilder()
                        .setSuccess(true)
                        .setMessage(
                                existing == null
                                        ? "Worker registered successfully"
                                        : "Worker was already registered"
                        )
                        .addAllActiveWorkers(
                                getActiveWorkersSnapshot()
                        )
                        .build();

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void unregisterWorker(
            WorkerIdRequest request,
            StreamObserver<Acknowledgement> responseObserver
    ) {
        if (request.getWorkerId() <= 0) {
            responseObserver.onError(
                    Status.INVALID_ARGUMENT
                            .withDescription(
                                    "Worker ID must be positive"
                            )
                            .asRuntimeException()
            );
            return;
        }

        WorkerInfo removed = activeWorkers.remove(
                request.getWorkerId()
        );

        Acknowledgement reply =
                Acknowledgement.newBuilder()
                        .setSuccess(removed != null)
                        .setMessage(
                                removed != null
                                        ? "Worker unregistered successfully"
                                        : "Worker was not registered"
                        )
                        .build();

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void listActiveWorkers(
            Empty request,
            StreamObserver<WorkerList> responseObserver
    ) {
        WorkerList reply = WorkerList.newBuilder()
                .addAllWorkers(
                        getActiveWorkersSnapshot()
                )
                .build();

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void getRandomWorker(
            Empty request,
            StreamObserver<WorkerInfo> responseObserver
    ) {
        List<WorkerInfo> workers =
                getActiveWorkersSnapshot();

        if (workers.isEmpty()) {
            responseObserver.onError(
                    Status.NOT_FOUND
                            .withDescription(
                                    "No active workers are available"
                            )
                            .asRuntimeException()
            );
            return;
        }

        WorkerInfo selectedWorker = workers.get(
                ThreadLocalRandom.current()
                        .nextInt(workers.size())
        );

        responseObserver.onNext(selectedWorker);
        responseObserver.onCompleted();
    }

    public void registerWorkerLocally(WorkerInfo worker) {
        if (!isValidWorker(worker)) {
            throw new IllegalArgumentException(
                    "Worker ID, host, port and name are required"
            );
        }

        WorkerInfo existing = activeWorkers.putIfAbsent(
                worker.getWorkerId(),
                worker
        );

        if (existing != null && !existing.equals(worker)) {
            throw new IllegalArgumentException(
                    "Worker ID "
                            + worker.getWorkerId()
                            + " is already registered"
            );
        }
    }

    public List<WorkerInfo> getActiveWorkersSnapshot() {
        List<WorkerInfo> workers =
                new ArrayList<>(activeWorkers.values());

        workers.sort(
                Comparator.comparingInt(
                        WorkerInfo::getWorkerId
                )
        );

        return List.copyOf(workers);
    }

    public boolean containsWorker(int workerId) {
        return activeWorkers.containsKey(workerId);
    }

    public int getActiveWorkerCount() {
        return activeWorkers.size();
    }

    public boolean removeWorkerIfUnchanged(
            WorkerInfo expectedWorker
    ) {
        if (expectedWorker == null) {
            return false;
        }

        return removeWorkerIfUnchanged(
                expectedWorker.getWorkerId(),
                expectedWorker
        );
    }

    public boolean removeWorkerIfUnchanged(
            int workerId,
            WorkerInfo expectedWorker
    ) {
        if (expectedWorker == null) {
            return false;
        }

        return activeWorkers.remove(
                workerId,
                expectedWorker
        );
    }

    private static boolean isValidWorker(
            WorkerInfo worker
    ) {
        return worker != null
                && worker.getWorkerId() > 0
                && !worker.getHost().isBlank()
                && worker.getGrpcPort() > 0
                && worker.getGrpcPort() <= 65_535
                && !worker.getWorkerName().isBlank();
    }
}