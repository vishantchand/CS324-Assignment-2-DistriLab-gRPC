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
import fj.ac.usp.cs324.distrilab.grpc.WorkerServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

public final class GrpcWorkerService
        extends WorkerServiceGrpc.WorkerServiceImplBase {

    private final WorkerInfo localWorker;
    private final WorkerMembershipManager membershipManager;
    private final IntSupplier coordinatorIdSupplier;
    private final LongSupplier coordinatorTermSupplier;

    public GrpcWorkerService(
            WorkerInfo localWorker,
            WorkerMembershipManager membershipManager
    ) {
        this(localWorker, membershipManager, () -> 0, () -> 0L);
    }

    public GrpcWorkerService(
            WorkerInfo localWorker,
            WorkerMembershipManager membershipManager,
            IntSupplier coordinatorIdSupplier,
            LongSupplier coordinatorTermSupplier
    ) {
        this.localWorker = Objects.requireNonNull(
                localWorker,
                "localWorker cannot be null"
        );
        this.membershipManager = Objects.requireNonNull(
                membershipManager,
                "membershipManager cannot be null"
        );
        this.coordinatorIdSupplier = Objects.requireNonNull(
                coordinatorIdSupplier,
                "coordinatorIdSupplier cannot be null"
        );
        this.coordinatorTermSupplier = Objects.requireNonNull(
                coordinatorTermSupplier,
                "coordinatorTermSupplier cannot be null"
        );

        validateWorker(localWorker);
    }

    @Override
    public void getWorkerInfo(
            Empty request,
            StreamObserver<WorkerInfo> responseObserver
    ) {
        respond(responseObserver, localWorker);
    }

    @Override
    public void ping(
            HeartbeatRequest request,
            StreamObserver<HeartbeatReply> responseObserver
    ) {
        HeartbeatReply reply = HeartbeatReply.newBuilder()
                .setAlive(true)
                .setWorker(localWorker)
                .setHealth(WorkerHealth.WORKER_HEALTH_ACTIVE)
                .setCoordinatorId(coordinatorIdSupplier.getAsInt())
                .setCoordinatorTerm(coordinatorTermSupplier.getAsLong())
                .setTimestampMs(System.currentTimeMillis())
                .build();

        respond(responseObserver, reply);
    }

    @Override
    public void addNeighbor(
            WorkerInfo request,
            StreamObserver<Acknowledgement> responseObserver
    ) {
        try {
            validateWorker(request);

            boolean changed = membershipManager.addNeighbour(request);

            String message = changed
                    ? "Worker " + request.getWorkerId()
                    + " added or updated"
                    : "Worker " + request.getWorkerId()
                    + " was already known or is the local worker";

            respond(
                    responseObserver,
                    acknowledgement(true, message)
            );
        } catch (IllegalArgumentException exception) {
            invalidArgument(responseObserver, exception.getMessage());
        }
    }

    @Override
    public void removeNeighbor(
            WorkerIdRequest request,
            StreamObserver<Acknowledgement> responseObserver
    ) {
        if (request.getWorkerId() <= 0) {
            invalidArgument(
                    responseObserver,
                    "worker_id must be greater than zero"
            );
            return;
        }

        boolean removed = membershipManager
                .removeNeighbour(request.getWorkerId())
                .isPresent();

        String message = removed
                ? "Worker " + request.getWorkerId() + " removed"
                : "Worker " + request.getWorkerId() + " was not present";

        respond(
                responseObserver,
                acknowledgement(true, message)
        );
    }

    @Override
    public void listNeighbors(
            Empty request,
            StreamObserver<WorkerList> responseObserver
    ) {
        WorkerList reply = WorkerList.newBuilder()
                .addAllWorkers(
                        membershipManager.getNeighboursSnapshot()
                )
                .build();

        respond(responseObserver, reply);
    }

    @Override
    public void joinNetwork(
            JoinNetworkRequest request,
            StreamObserver<JoinNetworkReply> responseObserver
    ) {
        if (!request.hasWorker()) {
            invalidArgument(
                    responseObserver,
                    "joining worker information is required"
            );
            return;
        }

        WorkerInfo joiningWorker = request.getWorker();

        try {
            validateWorker(joiningWorker);
            membershipManager.addNeighbour(joiningWorker);

            List<WorkerInfo> availableNeighbours =
                    new ArrayList<>();

            availableNeighbours.add(localWorker);

            membershipManager.getNeighboursSnapshot().stream()
                    .filter(worker ->
                            worker.getWorkerId()
                                    != joiningWorker.getWorkerId()
                    )
                    .forEach(availableNeighbours::add);

            availableNeighbours.sort(
                    Comparator.comparingInt(WorkerInfo::getWorkerId)
            );

            JoinNetworkReply reply = JoinNetworkReply.newBuilder()
                    .setSuccess(true)
                    .setMessage(
                            "Worker "
                                    + joiningWorker.getWorkerId()
                                    + " joined the network"
                    )
                    .addAllNeighbors(availableNeighbours)
                    .setCoordinatorId(
                            coordinatorIdSupplier.getAsInt()
                    )
                    .setCoordinatorTerm(
                            coordinatorTermSupplier.getAsLong()
                    )
                    .build();

            respond(responseObserver, reply);
        } catch (IllegalArgumentException exception) {
            invalidArgument(responseObserver, exception.getMessage());
        }
    }

    @Override
    public void reportWorkerFailure(
            WorkerFailureReport request,
            StreamObserver<Acknowledgement> responseObserver
    ) {
        if (request.getFailedWorkerId() <= 0) {
            invalidArgument(
                    responseObserver,
                    "failed_worker_id must be greater than zero"
            );
            return;
        }

        boolean removed = membershipManager
                .removeNeighbour(request.getFailedWorkerId())
                .isPresent();

        String message = removed
                ? "Failed worker "
                + request.getFailedWorkerId()
                + " removed after report from worker "
                + request.getReporterId()
                : "Failed worker "
                + request.getFailedWorkerId()
                + " was already absent";

        respond(
                responseObserver,
                acknowledgement(true, message)
        );
    }

    private static void validateWorker(WorkerInfo worker) {
        if (worker == null) {
            throw new IllegalArgumentException(
                    "worker cannot be null"
            );
        }

        if (worker.getWorkerId() <= 0) {
            throw new IllegalArgumentException(
                    "worker_id must be greater than zero"
            );
        }

        if (worker.getHost().isBlank()) {
            throw new IllegalArgumentException(
                    "worker host cannot be blank"
            );
        }

        if (worker.getGrpcPort() <= 0
                || worker.getGrpcPort() > 65_535) {
            throw new IllegalArgumentException(
                    "grpc_port must be between 1 and 65535"
            );
        }

        if (worker.getWorkerName().isBlank()) {
            throw new IllegalArgumentException(
                    "worker_name cannot be blank"
            );
        }
    }

    private static Acknowledgement acknowledgement(
            boolean success,
            String message
    ) {
        return Acknowledgement.newBuilder()
                .setSuccess(success)
                .setMessage(message)
                .build();
    }

    private static <T> void respond(
            StreamObserver<T> responseObserver,
            T response
    ) {
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private static void invalidArgument(
            StreamObserver<?> responseObserver,
            String message
    ) {
        responseObserver.onError(
                Status.INVALID_ARGUMENT
                        .withDescription(message)
                        .asRuntimeException()
        );
    }
}