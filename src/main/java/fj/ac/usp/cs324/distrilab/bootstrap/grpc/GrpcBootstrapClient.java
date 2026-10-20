package fj.ac.usp.cs324.distrilab.bootstrap.grpc;

import fj.ac.usp.cs324.distrilab.grpc.Acknowledgement;
import fj.ac.usp.cs324.distrilab.grpc.BootstrapServiceGrpc;
import fj.ac.usp.cs324.distrilab.grpc.Empty;
import fj.ac.usp.cs324.distrilab.grpc.RegisterWorkerReply;
import fj.ac.usp.cs324.distrilab.grpc.WorkerIdRequest;
import fj.ac.usp.cs324.distrilab.grpc.WorkerInfo;
import fj.ac.usp.cs324.distrilab.grpc.WorkerList;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public final class GrpcBootstrapClient implements AutoCloseable {

    private static final Duration DEFAULT_DEADLINE =
            Duration.ofSeconds(3);

    private static final Duration SHUTDOWN_TIMEOUT =
            Duration.ofSeconds(3);

    private final ManagedChannel channel;

    private final
    BootstrapServiceGrpc.BootstrapServiceBlockingStub stub;

    private final Duration deadline;
    private final boolean ownsChannel;

    public GrpcBootstrapClient(
            String host,
            int port
    ) {
        this(
                host,
                port,
                DEFAULT_DEADLINE
        );
    }

    public GrpcBootstrapClient(
            String host,
            int port,
            Duration deadline
    ) {
        this(
                createValidatedChannel(
                        host,
                        port,
                        deadline
                ),
                deadline,
                true
        );
    }

    GrpcBootstrapClient(
            ManagedChannel channel,
            Duration deadline
    ) {
        this(
                channel,
                deadline,
                false
        );
    }

    private GrpcBootstrapClient(
            ManagedChannel channel,
            Duration deadline,
            boolean ownsChannel
    ) {
        this.channel = Objects.requireNonNull(
                channel,
                "channel cannot be null"
        );

        this.deadline = validateDeadline(deadline);
        this.ownsChannel = ownsChannel;

        this.stub =
                BootstrapServiceGrpc.newBlockingStub(
                        channel
                );
    }

    public RegisterWorkerReply registerWorker(
            WorkerInfo worker
    ) {
        Objects.requireNonNull(
                worker,
                "worker cannot be null"
        );

        return deadlineStub().registerWorker(worker);
    }

    public Acknowledgement unregisterWorker(
            int workerId
    ) {
        validateWorkerId(workerId);

        WorkerIdRequest request =
                WorkerIdRequest.newBuilder()
                        .setWorkerId(workerId)
                        .build();

        return deadlineStub().unregisterWorker(request);
    }

    public List<WorkerInfo> listActiveWorkers() {
        WorkerList reply =
                deadlineStub().listActiveWorkers(
                        Empty.getDefaultInstance()
                );

        return List.copyOf(
                reply.getWorkersList()
        );
    }

    public Optional<WorkerInfo> getRandomWorker() {
        try {
            WorkerInfo worker =
                    deadlineStub().getRandomWorker(
                            Empty.getDefaultInstance()
                    );

            return Optional.of(worker);
        } catch (StatusRuntimeException exception) {
            if (exception.getStatus().getCode()
                    == Status.Code.NOT_FOUND) {
                return Optional.empty();
            }

            throw exception;
        }
    }

    public boolean isShutdown() {
        return channel.isShutdown();
    }

    public boolean isTerminated() {
        return channel.isTerminated();
    }

    @Override
    public void close() {
        if (!ownsChannel || channel.isShutdown()) {
            return;
        }

        channel.shutdown();

        try {
            boolean terminated =
                    channel.awaitTermination(
                            SHUTDOWN_TIMEOUT.toMillis(),
                            TimeUnit.MILLISECONDS
                    );

            if (!terminated) {
                channel.shutdownNow();

                channel.awaitTermination(
                        SHUTDOWN_TIMEOUT.toMillis(),
                        TimeUnit.MILLISECONDS
                );
            }
        } catch (InterruptedException exception) {
            channel.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private
    BootstrapServiceGrpc.BootstrapServiceBlockingStub
    deadlineStub() {
        return stub.withDeadlineAfter(
                deadline.toMillis(),
                TimeUnit.MILLISECONDS
        );
    }

    /*
     * Validate the complete configuration before allocating
     * a ManagedChannel. This prevents a channel from being
     * leaked when the deadline is invalid.
     */
    private static ManagedChannel createValidatedChannel(
            String host,
            int port,
            Duration deadline
    ) {
        validateDeadline(deadline);

        return createChannel(
                host,
                port
        );
    }

    private static ManagedChannel createChannel(
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

        return ManagedChannelBuilder
                .forAddress(
                        host.trim(),
                        port
                )
                .usePlaintext()
                .build();
    }

    private static Duration validateDeadline(
            Duration deadline
    ) {
        Objects.requireNonNull(
                deadline,
                "deadline cannot be null"
        );

        if (deadline.isZero()
                || deadline.isNegative()) {
            throw new IllegalArgumentException(
                    "deadline must be greater than zero"
            );
        }

        return deadline;
    }

    private static void validateWorkerId(
            int workerId
    ) {
        if (workerId <= 0) {
            throw new IllegalArgumentException(
                    "workerId must be greater than zero"
            );
        }
    }
}