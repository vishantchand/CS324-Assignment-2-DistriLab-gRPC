package fj.ac.usp.cs324.distrilab.worker.grpc;

import fj.ac.usp.cs324.distrilab.grpc.HeartbeatReply;
import fj.ac.usp.cs324.distrilab.grpc.WorkerHealth;
import fj.ac.usp.cs324.distrilab.grpc.WorkerInfo;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

public final class GrpcWorkerFailureDetector
        implements AutoCloseable {

    private static final Duration DEFAULT_INTERVAL =
            Duration.ofSeconds(5);

    private static final Duration DEFAULT_RPC_TIMEOUT =
            Duration.ofSeconds(2);

    private static final int DEFAULT_FAILURE_THRESHOLD = 3;

    private final int localWorkerId;
    private final WorkerMembershipManager membershipManager;
    private final IntSupplier coordinatorIdSupplier;
    private final LongSupplier coordinatorTermSupplier;
    private final Duration interval;
    private final int failureThreshold;
    private final WorkerProbe workerProbe;
    private final FailureReporter failureReporter;

    private final ConcurrentHashMap<Integer, Integer>
            missedHeartbeats = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<Integer, WorkerHealth>
            workerHealth = new ConcurrentHashMap<>();

    private ScheduledExecutorService scheduler;

    public GrpcWorkerFailureDetector(
            int localWorkerId,
            WorkerMembershipManager membershipManager,
            IntSupplier coordinatorIdSupplier,
            LongSupplier coordinatorTermSupplier
    ) {
        this(
                localWorkerId,
                membershipManager,
                coordinatorIdSupplier,
                coordinatorTermSupplier,
                DEFAULT_INTERVAL,
                DEFAULT_FAILURE_THRESHOLD,
                new GrpcWorkerProbe(DEFAULT_RPC_TIMEOUT),
                new GrpcFailureReporter(DEFAULT_RPC_TIMEOUT)
        );
    }

    GrpcWorkerFailureDetector(
            int localWorkerId,
            WorkerMembershipManager membershipManager,
            IntSupplier coordinatorIdSupplier,
            LongSupplier coordinatorTermSupplier,
            Duration interval,
            int failureThreshold,
            WorkerProbe workerProbe,
            FailureReporter failureReporter
    ) {
        if (localWorkerId <= 0) {
            throw new IllegalArgumentException(
                    "localWorkerId must be greater than zero"
            );
        }

        this.localWorkerId = localWorkerId;

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

        this.interval = validateInterval(interval);

        if (failureThreshold <= 0) {
            throw new IllegalArgumentException(
                    "failureThreshold must be greater than zero"
            );
        }

        this.failureThreshold = failureThreshold;

        this.workerProbe = Objects.requireNonNull(
                workerProbe,
                "workerProbe cannot be null"
        );

        this.failureReporter = Objects.requireNonNull(
                failureReporter,
                "failureReporter cannot be null"
        );
    }

    public synchronized void start() {
        if (isRunning()) {
            return;
        }

        scheduler = Executors.newSingleThreadScheduledExecutor(
                runnable -> {
                    Thread thread = new Thread(
                            runnable,
                            "grpc-worker-failure-detector-"
                                    + localWorkerId
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
        for (WorkerInfo neighbour
                : membershipManager.getNeighboursSnapshot()) {

            checkNeighbour(neighbour);
        }
    }

    public WorkerHealth getWorkerHealth(int workerId) {
        return workerHealth.getOrDefault(
                workerId,
                WorkerHealth.WORKER_HEALTH_UNSPECIFIED
        );
    }

    public int getMissedHeartbeatCount(int workerId) {
        return missedHeartbeats.getOrDefault(workerId, 0);
    }

    private void checkNeighbour(WorkerInfo neighbour) {
        boolean alive;

        try {
            alive = workerProbe.isAlive(
                    neighbour,
                    localWorkerId,
                    coordinatorIdSupplier.getAsInt(),
                    coordinatorTermSupplier.getAsLong()
            );
        } catch (RuntimeException exception) {
            alive = false;
        }

        if (alive) {
            missedHeartbeats.remove(neighbour.getWorkerId());

            workerHealth.put(
                    neighbour.getWorkerId(),
                    WorkerHealth.WORKER_HEALTH_ACTIVE
            );

            return;
        }

        int misses = missedHeartbeats.merge(
                neighbour.getWorkerId(),
                1,
                Integer::sum
        );

        if (misses < failureThreshold) {
            workerHealth.put(
                    neighbour.getWorkerId(),
                    WorkerHealth.WORKER_HEALTH_SUSPECTED
            );

            System.out.println(
                    "Worker " + neighbour.getWorkerId()
                            + " is suspected after "
                            + misses + " missed heartbeat(s)"
            );

            return;
        }

        workerHealth.put(
                neighbour.getWorkerId(),
                WorkerHealth.WORKER_HEALTH_FAILED
        );

        missedHeartbeats.remove(neighbour.getWorkerId());

        boolean removed = membershipManager
                .removeNeighbour(neighbour.getWorkerId())
                .isPresent();

        if (removed) {
            System.out.println(
                    "Worker " + neighbour.getWorkerId()
                            + " marked failed and removed"
            );

            reportFailureToRemainingNeighbours(
                    neighbour.getWorkerId()
            );
        }
    }

    private void reportFailureToRemainingNeighbours(
            int failedWorkerId
    ) {
        for (WorkerInfo recipient
                : membershipManager.getNeighboursSnapshot()) {

            try {
                failureReporter.report(
                        recipient,
                        localWorkerId,
                        failedWorkerId,
                        "Worker failed after "
                                + failureThreshold
                                + " missed heartbeats"
                );
            } catch (RuntimeException exception) {
                System.err.println(
                        "Unable to report Worker "
                                + failedWorkerId
                                + " failure to Worker "
                                + recipient.getWorkerId()
                );
            }
        }
    }

    private void runSafely() {
        try {
            runHealthCheckNow();
        } catch (RuntimeException exception) {
            System.err.println(
                    "Worker failure detector error: "
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
    interface WorkerProbe {

        boolean isAlive(
                WorkerInfo worker,
                int senderId,
                int coordinatorId,
                long coordinatorTerm
        );
    }

    @FunctionalInterface
    interface FailureReporter {

        void report(
                WorkerInfo recipient,
                int reporterId,
                int failedWorkerId,
                String reason
        );
    }

    private static final class GrpcWorkerProbe
            implements WorkerProbe {

        private final Duration timeout;

        private GrpcWorkerProbe(Duration timeout) {
            this.timeout = timeout;
        }

        @Override
        public boolean isAlive(
                WorkerInfo worker,
                int senderId,
                int coordinatorId,
                long coordinatorTerm
        ) {
            GrpcWorkerClient client = new GrpcWorkerClient(
                    worker.getHost(),
                    worker.getGrpcPort(),
                    timeout
            );

            try {
                HeartbeatReply reply = client.ping(
                        senderId,
                        coordinatorId,
                        coordinatorTerm
                );

                return reply.getAlive();
            } finally {
                client.close();
            }
        }
    }

    private static final class GrpcFailureReporter
            implements FailureReporter {

        private final Duration timeout;

        private GrpcFailureReporter(Duration timeout) {
            this.timeout = timeout;
        }

        @Override
        public void report(
                WorkerInfo recipient,
                int reporterId,
                int failedWorkerId,
                String reason
        ) {
            GrpcWorkerClient client = new GrpcWorkerClient(
                    recipient.getHost(),
                    recipient.getGrpcPort(),
                    timeout
            );

            try {
                client.reportWorkerFailure(
                        reporterId,
                        failedWorkerId,
                        reason
                );
            } finally {
                client.close();
            }
        }
    }
}