package fj.ac.usp.cs324.distrilab.worker.grpc;

import fj.ac.usp.cs324.distrilab.bootstrap.grpc.GrpcBootstrapClient;
import fj.ac.usp.cs324.distrilab.grpc.JoinNetworkReply;
import fj.ac.usp.cs324.distrilab.grpc.RegisterWorkerReply;
import fj.ac.usp.cs324.distrilab.grpc.WorkerInfo;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.StatusRuntimeException;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class GrpcWorkerServer implements AutoCloseable {

    private static final long SHUTDOWN_TIMEOUT_SECONDS = 5L;

    private final WorkerInfo localWorker;
    private final WorkerMembershipManager membershipManager;
    private final AtomicInteger coordinatorId;
    private final AtomicLong coordinatorTerm;

    private final GrpcBootstrapClient bootstrapClient;
    private final GrpcWorkerFailureDetector failureDetector;
    private final Server server;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public GrpcWorkerServer(
            int workerId,
            String workerHost,
            int workerPort,
            String bootstrapHost,
            int bootstrapPort
    ) {
        validateArguments(
                workerId,
                workerHost,
                workerPort,
                bootstrapHost,
                bootstrapPort
        );

        localWorker = WorkerInfo.newBuilder()
                .setWorkerId(workerId)
                .setHost(workerHost)
                .setGrpcPort(workerPort)
                .setWorkerName("Worker-" + workerId)
                .build();

        membershipManager =
                new WorkerMembershipManager(workerId);

        coordinatorId = new AtomicInteger(0);
        coordinatorTerm = new AtomicLong(0L);

        GrpcWorkerService workerService =
                new GrpcWorkerService(
                        localWorker,
                        membershipManager,
                        coordinatorId::get,
                        coordinatorTerm::get
                );

        JobGrpcService jobService =
                new JobGrpcService(workerId);

        server = ServerBuilder
                .forPort(workerPort)
                .addService(workerService)
                .addService(jobService)
                .build();


        bootstrapClient = new GrpcBootstrapClient(
                bootstrapHost,
                bootstrapPort
        );

        failureDetector =
                new GrpcWorkerFailureDetector(
                        workerId,
                        membershipManager,
                        coordinatorId::get,
                        coordinatorTerm::get
                );
    }

    public synchronized void start() throws IOException {
        if (closed.get()) {
            throw new IllegalStateException(
                    "Worker server has already been closed"
            );
        }

        if (!started.compareAndSet(false, true)) {
            return;
        }

        try {
            server.start();

            RegisterWorkerReply registration =
                    bootstrapClient.registerWorker(localWorker);

            if (!registration.getSuccess()) {
                throw new IllegalStateException(
                        "Bootstrap registration failed: "
                                + registration.getMessage()
                );
            }

            joinExistingWorkers(
                    registration.getActiveWorkersList()
            );

            failureDetector.start();

            System.out.println(
                    "gRPC Worker Node started successfully."
            );
            System.out.println(
                    "Worker ID: " + localWorker.getWorkerId()
            );
            System.out.println(
                    "Host: " + localWorker.getHost()
            );
            System.out.println(
                    "gRPC port: " + localWorker.getGrpcPort()
            );
            System.out.println(
                    "Worker name: "
                            + localWorker.getWorkerName()
            );
            System.out.println(
                    "Active neighbours: "
                            + membershipManager.size()
            );
            System.out.println(
                    "Coordinator ID: " + coordinatorId.get()
            );
            System.out.println(
                    "Coordinator term: " + coordinatorTerm.get()
            );
            System.out.println(
                    "Worker heartbeat failure detection started."
            );
        } catch (IOException | RuntimeException exception) {
            started.set(false);
            stopAfterFailedStartup();
            throw exception;
        }
    }

    private void joinExistingWorkers(
            List<WorkerInfo> activeWorkers
    ) {
        for (WorkerInfo worker : activeWorkers) {
            if (worker.getWorkerId()
                    == localWorker.getWorkerId()) {
                continue;
            }

            membershipManager.addNeighbour(worker);

            try (GrpcWorkerClient workerClient =
                         new GrpcWorkerClient(
                                 worker.getHost(),
                                 worker.getGrpcPort()
                         )) {

                JoinNetworkReply reply =
                        workerClient.joinNetwork(localWorker);

                if (!reply.getSuccess()) {
                    System.err.println(
                            "Worker "
                                    + worker.getWorkerId()
                                    + " rejected join request: "
                                    + reply.getMessage()
                    );
                    continue;
                }

                addDiscoveredNeighbours(
                        reply.getNeighborsList()
                );

                updateCoordinatorState(
                        reply.getCoordinatorId(),
                        reply.getCoordinatorTerm()
                );

                System.out.println(
                        "Joined worker "
                                + worker.getWorkerId()
                                + " at "
                                + worker.getHost()
                                + ":"
                                + worker.getGrpcPort()
                );
            } catch (StatusRuntimeException exception) {
                membershipManager.removeNeighbour(
                        worker.getWorkerId()
                );

                System.err.println(
                        "Could not contact worker "
                                + worker.getWorkerId()
                                + ": "
                                + exception.getStatus()
                                .getDescription()
                );
            }
        }

        notifyAllNeighbours();
    }

    private void addDiscoveredNeighbours(
            List<WorkerInfo> discoveredWorkers
    ) {
        for (WorkerInfo worker : discoveredWorkers) {
            if (worker.getWorkerId()
                    != localWorker.getWorkerId()) {
                membershipManager.addNeighbour(worker);
            }
        }
    }

    private void notifyAllNeighbours() {
        for (WorkerInfo worker :
                membershipManager.getNeighboursSnapshot()) {

            try (GrpcWorkerClient workerClient =
                         new GrpcWorkerClient(
                                 worker.getHost(),
                                 worker.getGrpcPort()
                         )) {

                workerClient.addNeighbor(localWorker);
            } catch (StatusRuntimeException exception) {
                System.err.println(
                        "Could not notify worker "
                                + worker.getWorkerId()
                                + " about worker "
                                + localWorker.getWorkerId()
                );
            }
        }
    }

    public void updateCoordinatorState(
            int newCoordinatorId,
            long newCoordinatorTerm
    ) {
        if (newCoordinatorId <= 0
                || newCoordinatorTerm < 0) {
            return;
        }

        while (true) {
            long currentTerm = coordinatorTerm.get();

            if (newCoordinatorTerm < currentTerm) {
                return;
            }

            if (newCoordinatorTerm == currentTerm) {
                coordinatorId.set(newCoordinatorId);
                return;
            }

            if (coordinatorTerm.compareAndSet(
                    currentTerm,
                    newCoordinatorTerm
            )) {
                coordinatorId.set(newCoordinatorId);
                return;
            }
        }
    }

    public void awaitTermination()
            throws InterruptedException {
        server.awaitTermination();
    }

    public WorkerInfo getLocalWorker() {
        return localWorker;
    }

    public List<WorkerInfo> getNeighboursSnapshot() {
        return membershipManager.getNeighboursSnapshot();
    }

    public int getCoordinatorId() {
        return coordinatorId.get();
    }

    public long getCoordinatorTerm() {
        return coordinatorTerm.get();
    }

    public boolean isStarted() {
        return started.get()
                && !server.isShutdown();
    }

    private void stopAfterFailedStartup() {
        try {
            bootstrapClient.unregisterWorker(
                    localWorker.getWorkerId()
            );
        } catch (RuntimeException ignored) {
            // Startup already failed, so continue cleanup.
        }

        failureDetector.stop();
        server.shutdownNow();
        bootstrapClient.close();
    }

    @Override
    public synchronized void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        failureDetector.stop();

        notifyNeighboursOfShutdown();

        try {
            bootstrapClient.unregisterWorker(
                    localWorker.getWorkerId()
            );
        } catch (RuntimeException exception) {
            System.err.println(
                    "Could not unregister worker "
                            + localWorker.getWorkerId()
                            + " from Bootstrap: "
                            + exception.getMessage()
            );
        } finally {
            bootstrapClient.close();
        }

        server.shutdown();

        try {
            if (!server.awaitTermination(
                    SHUTDOWN_TIMEOUT_SECONDS,
                    TimeUnit.SECONDS
            )) {
                server.shutdownNow();
            }
        } catch (InterruptedException exception) {
            server.shutdownNow();
            Thread.currentThread().interrupt();
        }

        membershipManager.clear();
        started.set(false);

        System.out.println(
                "gRPC Worker "
                        + localWorker.getWorkerId()
                        + " stopped."
        );
    }

    private void notifyNeighboursOfShutdown() {
        for (WorkerInfo worker :
                membershipManager.getNeighboursSnapshot()) {

            try (GrpcWorkerClient workerClient =
                         new GrpcWorkerClient(
                                 worker.getHost(),
                                 worker.getGrpcPort()
                         )) {

                workerClient.removeNeighbor(
                        localWorker.getWorkerId()
                );
            } catch (RuntimeException exception) {
                System.err.println(
                        "Could not notify worker "
                                + worker.getWorkerId()
                                + " during shutdown."
                );
            }
        }
    }

    private static void validateArguments(
            int workerId,
            String workerHost,
            int workerPort,
            String bootstrapHost,
            int bootstrapPort
    ) {
        if (workerId <= 0) {
            throw new IllegalArgumentException(
                    "workerId must be positive"
            );
        }

        requireHost(workerHost, "workerHost");
        requirePort(workerPort, "workerPort");
        requireHost(bootstrapHost, "bootstrapHost");
        requirePort(bootstrapPort, "bootstrapPort");
    }

    private static void requireHost(
            String host,
            String parameterName
    ) {
        Objects.requireNonNull(
                host,
                parameterName + " cannot be null"
        );

        if (host.isBlank()) {
            throw new IllegalArgumentException(
                    parameterName + " cannot be blank"
            );
        }
    }

    private static void requirePort(
            int port,
            String parameterName
    ) {
        if (port <= 0 || port > 65_535) {
            throw new IllegalArgumentException(
                    parameterName
                            + " must be between 1 and 65535"
            );
        }
    }

    public static void main(String[] args) {
        if (args.length != 5) {
            System.err.println(
                    "Usage: GrpcWorkerServer "
                            + "<workerId> <workerHost> "
                            + "<workerPort> "
                            + "<bootstrapHost> <bootstrapPort>"
            );
            return;
        }

        try {
            int workerId = Integer.parseInt(args[0]);
            String workerHost = args[1];
            int workerPort = Integer.parseInt(args[2]);
            String bootstrapHost = args[3];
            int bootstrapPort = Integer.parseInt(args[4]);

            GrpcWorkerServer workerServer =
                    new GrpcWorkerServer(
                            workerId,
                            workerHost,
                            workerPort,
                            bootstrapHost,
                            bootstrapPort
                    );

            Runtime.getRuntime().addShutdownHook(
                    new Thread(
                            workerServer::close,
                            "grpc-worker-shutdown-"
                                    + workerId
                    )
            );

            workerServer.start();
            workerServer.awaitTermination();
        } catch (NumberFormatException exception) {
            System.err.println(
                    "Worker ID and ports must be numbers."
            );
        } catch (Exception exception) {
            System.err.println(
                    "Could not start gRPC Worker: "
                            + exception.getMessage()
            );
            exception.printStackTrace();
        }
    }
}