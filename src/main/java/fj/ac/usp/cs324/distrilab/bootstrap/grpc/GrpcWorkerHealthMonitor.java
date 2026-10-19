package fj.ac.usp.cs324.distrilab.bootstrap.grpc;

import fj.ac.usp.cs324.distrilab.grpc.HeartbeatReply;
import fj.ac.usp.cs324.distrilab.grpc.HeartbeatRequest;
import fj.ac.usp.cs324.distrilab.grpc.WorkerInfo;
import fj.ac.usp.cs324.distrilab.grpc.WorkerServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class GrpcWorkerHealthMonitor
        implements AutoCloseable {

    private static final Duration DEFAULT_INTERVAL =
            Duration.ofSeconds(5);

    private static final Duration DEFAULT_RPC_DEADLINE =
            Duration.ofSeconds(2);

    private static final int DEFAULT_FAILURE_THRESHOLD = 3;

    private final GrpcBootstrapService bootstrapService;
    private final Duration interval;
    private final int failureThreshold;
    private final WorkerPinger workerPinger;

    private final ConcurrentHashMap<Integer, Integer>
            consecutiveFailures;

    private ScheduledExecutorService scheduler;

    public GrpcWorkerHealthMonitor(
            GrpcBootstrapService bootstrapService
    ) {
        this(
                bootstrapService,
                DEFAULT_INTERVAL,
                DEFAULT_FAILURE_THRESHOLD,
                new GrpcWorkerPinger(DEFAULT_RPC_DEADLINE)
        );
    }

    GrpcWorkerHealthMonitor(
            GrpcBootstrapService bootstrapService,
            Duration interval,
            int failureThreshold,
            WorkerPinger workerPinger
    ) {
        this.bootstrapService = Objects.requireNonNull(
                bootstrapService,
                "bootstrapService cannot be null"
        );

        this.interval = validateInterval(interval);

        if (failureThreshold <= 0) {
            throw new IllegalArgumentException(
                    "failureThreshold must be greater than zero"
            );
        }

        this.failureThreshold = failureThreshold;
        this.workerPinger = Objects.requireNonNull(
                workerPinger,
                "workerPinger cannot be null"
        );

        consecutiveFailures = new ConcurrentHashMap<>();
    }

    public synchronized void start() {
        if (isRunning()) {
            return;
        }

        scheduler = Executors.newSingleThreadScheduledExecutor(
                runnable -> {
                    Thread thread = new Thread(
                            runnable,
                            "grpc-bootstrap-health-monitor"
                    );

                    thread.setDaemon(true);
                    return thread;
                }
        );

        scheduler.scheduleWithFixedDelay(
                this::runSafely,
                interval.toMillis(),
                interval.toMillis(),
                TimeUnit.MILLISECONDS
        );
    }

    public synchronized boolean isRunning() {
        return scheduler != null
                && !scheduler.isShutdown()
                && !scheduler.isTerminated();
    }

    public void runHealthCheckNow() {
        for (WorkerInfo worker
                : bootstrapService.getActiveWorkersSnapshot()) {

            checkWorker(worker);
        }

        consecutiveFailures.keySet().removeIf(
                workerId -> !bootstrapService.containsWorker(workerId)
        );
    }

    public int getConsecutiveFailures(int workerId) {
        return consecutiveFailures.getOrDefault(workerId, 0);
    }

    private void checkWorker(WorkerInfo worker) {
        boolean reachable;

        try {
            reachable = workerPinger.ping(worker);
        } catch (RuntimeException exception) {
            reachable = false;
        }

        if (reachable) {
            consecutiveFailures.remove(worker.getWorkerId());
            return;
        }

        int failures = consecutiveFailures.merge(
                worker.getWorkerId(),
                1,
                Integer::sum
        );

        if (failures < failureThreshold) {
            System.out.println(
                    "Worker " + worker.getWorkerId()
                            + " missed health check "
                            + failures + "/"
                            + failureThreshold
            );
            return;
        }

        boolean removed =
                bootstrapService.removeWorkerIfUnchanged(worker);

        consecutiveFailures.remove(worker.getWorkerId());

        if (removed) {
            System.out.println(
                    "Removed unreachable worker from "
                            + "gRPC Bootstrap Node: "
                            + worker.getWorkerId()
            );
        }
    }

    private void runSafely() {
        try {
            runHealthCheckNow();
        } catch (RuntimeException exception) {
            System.err.println(
                    "Bootstrap health check failed: "
                            + exception.getMessage()
            );
        }
    }

    public synchronized void stop() {
        if (scheduler == null || scheduler.isShutdown()) {
            return;
        }

        scheduler.shutdownNow();

        try {
            scheduler.awaitTermination(
                    2,
                    TimeUnit.SECONDS
            );
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        stop();
    }

    private static Duration validateInterval(
            Duration interval
    ) {
        Objects.requireNonNull(
                interval,
                "interval cannot be null"
        );

        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException(
                    "interval must be greater than zero"
            );
        }

        return interval;
    }

    @FunctionalInterface
    interface WorkerPinger {

        boolean ping(WorkerInfo worker);
    }

    private static final class GrpcWorkerPinger
            implements WorkerPinger {

        private final Duration deadline;

        private GrpcWorkerPinger(Duration deadline) {
            this.deadline = deadline;
        }

        @Override
        public boolean ping(WorkerInfo worker) {
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress(
                            worker.getHost(),
                            worker.getGrpcPort()
                    )
                    .usePlaintext()
                    .build();

            try {
                HeartbeatRequest request =
                        HeartbeatRequest.newBuilder()
                                .setSenderId(0)
                                .setKnownCoordinatorId(0)
                                .setCoordinatorTerm(0)
                                .setTimestampMs(
                                        System.currentTimeMillis()
                                )
                                .build();

                HeartbeatReply reply = WorkerServiceGrpc
                        .newBlockingStub(channel)
                        .withDeadlineAfter(
                                deadline.toMillis(),
                                TimeUnit.MILLISECONDS
                        )
                        .ping(request);

                return reply.getAlive();
            } catch (StatusRuntimeException exception) {
                return false;
            } finally {
                channel.shutdownNow();

                try {
                    channel.awaitTermination(
                            1,
                            TimeUnit.SECONDS
                    );
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}