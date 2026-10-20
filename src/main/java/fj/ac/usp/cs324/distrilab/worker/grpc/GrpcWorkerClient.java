package fj.ac.usp.cs324.distrilab.worker.grpc;

import fj.ac.usp.cs324.distrilab.grpc.Acknowledgement;
import fj.ac.usp.cs324.distrilab.grpc.Empty;
import fj.ac.usp.cs324.distrilab.grpc.HeartbeatReply;
import fj.ac.usp.cs324.distrilab.grpc.HeartbeatRequest;
import fj.ac.usp.cs324.distrilab.grpc.JoinNetworkReply;
import fj.ac.usp.cs324.distrilab.grpc.JoinNetworkRequest;
import fj.ac.usp.cs324.distrilab.grpc.WorkerFailureReport;
import fj.ac.usp.cs324.distrilab.grpc.WorkerIdRequest;
import fj.ac.usp.cs324.distrilab.grpc.WorkerInfo;
import fj.ac.usp.cs324.distrilab.grpc.WorkerList;
import fj.ac.usp.cs324.distrilab.grpc.WorkerServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public final class GrpcWorkerClient implements AutoCloseable {

    private static final Duration DEFAULT_TIMEOUT =
            Duration.ofSeconds(3);

    private final ManagedChannel channel;
    private final Duration timeout;

    public GrpcWorkerClient(String host, int port) {
        this(host, port, DEFAULT_TIMEOUT);
    }

    public GrpcWorkerClient(
            String host,
            int port,
            Duration timeout
    ) {
        validateEndpoint(host, port);
        validateTimeout(timeout);

        this.timeout = timeout;
        this.channel = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext()
                .build();
    }

    public WorkerInfo getWorkerInfo() {
        return stub()
                .getWorkerInfo(
                        Empty.getDefaultInstance()
                );
    }

    public HeartbeatReply ping(
            int senderId,
            int knownCoordinatorId,
            long coordinatorTerm
    ) {
        HeartbeatRequest request =
                HeartbeatRequest.newBuilder()
                        .setSenderId(senderId)
                        .setKnownCoordinatorId(
                                knownCoordinatorId
                        )
                        .setCoordinatorTerm(
                                coordinatorTerm
                        )
                        .setTimestampMs(
                                System.currentTimeMillis()
                        )
                        .build();

        return stub().ping(request);
    }

    public Acknowledgement addNeighbor(
            WorkerInfo worker
    ) {
        Objects.requireNonNull(
                worker,
                "worker cannot be null"
        );

        return stub().addNeighbor(worker);
    }

    public Acknowledgement removeNeighbor(
            int workerId
    ) {
        WorkerIdRequest request =
                WorkerIdRequest.newBuilder()
                        .setWorkerId(workerId)
                        .build();

        return stub().removeNeighbor(request);
    }

    public List<WorkerInfo> listNeighbors() {
        WorkerList reply = stub()
                .listNeighbors(
                        Empty.getDefaultInstance()
                );

        return List.copyOf(reply.getWorkersList());
    }

    public JoinNetworkReply joinNetwork(
            WorkerInfo worker
    ) {
        Objects.requireNonNull(
                worker,
                "worker cannot be null"
        );

        JoinNetworkRequest request =
                JoinNetworkRequest.newBuilder()
                        .setWorker(worker)
                        .build();

        return stub().joinNetwork(request);
    }

    public Acknowledgement reportWorkerFailure(
            int reporterId,
            int failedWorkerId,
            String reason
    ) {
        WorkerFailureReport request =
                WorkerFailureReport.newBuilder()
                        .setReporterId(reporterId)
                        .setFailedWorkerId(
                                failedWorkerId
                        )
                        .setDetectedAtMs(
                                System.currentTimeMillis()
                        )
                        .setReason(
                                reason == null ? "" : reason
                        )
                        .build();

        return stub().reportWorkerFailure(request);
    }

    private WorkerServiceGrpc.WorkerServiceBlockingStub stub() {
        return WorkerServiceGrpc
                .newBlockingStub(channel)
                .withDeadlineAfter(
                        timeout.toMillis(),
                        TimeUnit.MILLISECONDS
                );
    }

    @Override
    public void close() {
        channel.shutdown();

        try {
            if (!channel.awaitTermination(
                    timeout.toMillis(),
                    TimeUnit.MILLISECONDS
            )) {
                channel.shutdownNow();
            }
        } catch (InterruptedException exception) {
            channel.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static void validateEndpoint(
            String host,
            int port
    ) {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException(
                    "host cannot be blank"
            );
        }

        if (port < 1 || port > 65_535) {
            throw new IllegalArgumentException(
                    "port must be between 1 and 65535"
            );
        }
    }

    private static void validateTimeout(Duration timeout) {
        if (timeout == null
                || timeout.isZero()
                || timeout.isNegative()) {
            throw new IllegalArgumentException(
                    "timeout must be positive"
            );
        }
    }
}