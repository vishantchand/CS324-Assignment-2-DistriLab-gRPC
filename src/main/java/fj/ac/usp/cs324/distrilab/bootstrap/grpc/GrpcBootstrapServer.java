package fj.ac.usp.cs324.distrilab.bootstrap.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public final class GrpcBootstrapServer implements AutoCloseable {

    public static final int DEFAULT_PORT = 50051;

    private static final long SHUTDOWN_TIMEOUT_SECONDS = 5L;

    private final int requestedPort;
    private final GrpcBootstrapService bootstrapService;
    private final GrpcWorkerHealthMonitor healthMonitor;

    private Server server;

    public GrpcBootstrapServer(int port) {
        validatePort(port);

        requestedPort = port;
        bootstrapService = new GrpcBootstrapService();
        healthMonitor = new GrpcWorkerHealthMonitor(
                bootstrapService
        );
    }

    GrpcBootstrapServer(
            int port,
            GrpcBootstrapService bootstrapService
    ) {
        validatePort(port);

        requestedPort = port;

        this.bootstrapService = Objects.requireNonNull(
                bootstrapService,
                "bootstrapService cannot be null"
        );

        healthMonitor = new GrpcWorkerHealthMonitor(
                bootstrapService
        );
    }

    public synchronized void start() throws IOException {
        if (isRunning()) {
            throw new IllegalStateException(
                    "Bootstrap gRPC server is already running"
            );
        }

        server = ServerBuilder
                .forPort(requestedPort)
                .addService(bootstrapService)
                .build()
                .start();

        healthMonitor.start();
    }

    public synchronized boolean isRunning() {
        return server != null
                && !server.isShutdown()
                && !server.isTerminated();
    }

    public boolean isHealthMonitorRunning() {
        return healthMonitor.isRunning();
    }

    public synchronized int getPort() {
        if (server == null) {
            return requestedPort;
        }

        return server.getPort();
    }

    public GrpcBootstrapService getBootstrapService() {
        return bootstrapService;
    }

    public void awaitTermination() throws InterruptedException {
        Server currentServer;

        synchronized (this) {
            currentServer = server;
        }

        if (currentServer != null) {
            currentServer.awaitTermination();
        }
    }

    public synchronized void stop() {
        healthMonitor.stop();

        if (server == null || server.isShutdown()) {
            return;
        }

        server.shutdown();

        try {
            boolean terminated = server.awaitTermination(
                    SHUTDOWN_TIMEOUT_SECONDS,
                    TimeUnit.SECONDS
            );

            if (!terminated) {
                server.shutdownNow();

                server.awaitTermination(
                        SHUTDOWN_TIMEOUT_SECONDS,
                        TimeUnit.SECONDS
                );
            }
        } catch (InterruptedException exception) {
            server.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        stop();
    }

    public static void main(String[] args) {
        int port;

        try {
            port = parsePort(args);
        } catch (IllegalArgumentException exception) {
            System.err.println(
                    "Unable to start Bootstrap Node: "
                            + exception.getMessage()
            );
            return;
        }

        GrpcBootstrapServer bootstrapServer =
                new GrpcBootstrapServer(port);

        Runtime.getRuntime().addShutdownHook(
                new Thread(
                        bootstrapServer::stop,
                        "grpc-bootstrap-shutdown"
                )
        );

        try {
            bootstrapServer.start();

            System.out.println(
                    "Bootstrap Node started successfully using gRPC."
            );
            System.out.println(
                    "gRPC port: " + bootstrapServer.getPort()
            );
            System.out.println(
                    "Worker health-check interval: 5 seconds"
            );
            System.out.println(
                    "Failure threshold: 3 missed health checks"
            );
            System.out.println(
                    "Waiting for worker registrations..."
            );

            bootstrapServer.awaitTermination();
        } catch (IOException exception) {
            System.err.println(
                    "Unable to start Bootstrap Node: "
                            + exception.getMessage()
            );
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        } finally {
            bootstrapServer.stop();
        }
    }

    private static int parsePort(String[] args) {
        if (args == null || args.length == 0) {
            return DEFAULT_PORT;
        }

        if (args.length > 1) {
            throw new IllegalArgumentException(
                    "Usage: GrpcBootstrapServer [port]"
            );
        }

        try {
            int port = Integer.parseInt(args[0]);

            if (port < 1 || port > 65_535) {
                throw new IllegalArgumentException(
                        "port must be between 1 and 65535"
                );
            }

            return port;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(
                    "port must be a valid integer",
                    exception
            );
        }
    }

    private static void validatePort(int port) {
        if (port < 0 || port > 65_535) {
            throw new IllegalArgumentException(
                    "port must be between 0 and 65535"
            );
        }
    }
}