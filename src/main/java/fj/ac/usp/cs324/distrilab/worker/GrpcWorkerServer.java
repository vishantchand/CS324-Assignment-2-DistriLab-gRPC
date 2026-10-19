package fj.ac.usp.cs324.distrilab.worker;

import fj.ac.usp.cs324.distrilab.election.ElectionManager;
import fj.ac.usp.cs324.distrilab.election.ElectionServiceImpl;
import fj.ac.usp.cs324.distrilab.grpc.WorkerInfo;
import fj.ac.usp.cs324.distrilab.recovery.RecoveryServiceImpl;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public final class GrpcWorkerServer {

    private static final int DEFAULT_INITIAL_JAC = 0;

    private final int workerId;
    private final int grpcPort;
    private final Server server;

    public GrpcWorkerServer(
            int workerId,
            String host,
            int grpcPort
    ) {
        this(
                workerId,
                host,
                grpcPort,
                DEFAULT_INITIAL_JAC
        );
    }

    public GrpcWorkerServer(
            int workerId,
            String host,
            int grpcPort,
            int initialJac
    ) {
        validateWorkerId(workerId);
        validateHost(host);
        validatePort(grpcPort);
        validateInitialJac(initialJac);

        this.workerId = workerId;
        this.grpcPort = grpcPort;

        WorkerInfo workerInfo = WorkerInfo.newBuilder()
                .setWorkerId(workerId)
                .setHost(host)
                .setGrpcPort(grpcPort)
                .setWorkerName("Worker-" + workerId)
                .build();

        ElectionManager electionManager =
                new ElectionManager(
                        workerId,
                        initialJac
                );

        ElectionServiceImpl electionService =
                new ElectionServiceImpl(
                        electionManager,
                        workerInfo
                );

        RecoveryServiceImpl recoveryService =
                new RecoveryServiceImpl();

        this.server = ServerBuilder
                .forPort(grpcPort)
                .addService(electionService)
                .addService(recoveryService)
                .build();
    }

    public void start() throws IOException {
        server.start();

        System.out.println(
                "gRPC Worker Server started successfully."
        );
        System.out.println(
                "Worker ID: " + workerId
        );
        System.out.println(
                "gRPC port: " + server.getPort()
        );

        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    System.out.println(
                            "Shutting down gRPC Worker Server..."
                    );

                    stop();
                })
        );
    }

    public void stop() {
        server.shutdown();

        try {
            if (!server.awaitTermination(
                    5,
                    TimeUnit.SECONDS
            )) {
                server.shutdownNow();
            }
        } catch (InterruptedException exception) {
            server.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public void blockUntilShutdown()
            throws InterruptedException {
        server.awaitTermination();
    }

    public int getPort() {
        return server.getPort();
    }

    public boolean isShutdown() {
        return server.isShutdown();
    }

    public static void main(String[] args) {
        if (args.length != 3 && args.length != 4) {
            printUsage();
            return;
        }

        try {
            int workerId = parseInteger(
                    args[0],
                    "worker ID"
            );

            String host = args[1];

            int grpcPort = parsePort(
                    args[2],
                    "gRPC port"
            );

            int initialJac =
                    args.length == 4
                            ? parseInteger(
                            args[3],
                            "initial JAC"
                    )
                            : DEFAULT_INITIAL_JAC;

            GrpcWorkerServer workerServer =
                    new GrpcWorkerServer(
                            workerId,
                            host,
                            grpcPort,
                            initialJac
                    );

            workerServer.start();
            workerServer.blockUntilShutdown();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();

            System.err.println(
                    "gRPC Worker Server was interrupted."
            );
        } catch (Exception exception) {
            System.err.println(
                    "Unable to start gRPC Worker Server: "
                            + exception.getMessage()
            );

            exception.printStackTrace();
        }
    }

    private static int parseInteger(
            String value,
            String fieldName
    ) {
        try {
            int number = Integer.parseInt(value);

            if (number < 0) {
                throw new IllegalArgumentException(
                        fieldName + " cannot be negative"
                );
            }

            return number;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(
                    fieldName + " must be a number"
            );
        }
    }

    private static int parsePort(
            String value,
            String fieldName
    ) {
        int port = parseInteger(
                value,
                fieldName
        );

        validatePort(port);
        return port;
    }

    private static void validateWorkerId(int workerId) {
        if (workerId < 0) {
            throw new IllegalArgumentException(
                    "worker ID cannot be negative"
            );
        }
    }

    private static void validateHost(String host) {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException(
                    "host cannot be blank"
            );
        }
    }

    private static void validatePort(int port) {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException(
                    "gRPC port must be between 1 and 65535"
            );
        }
    }

    private static void validateInitialJac(int initialJac) {
        if (initialJac < 0) {
            throw new IllegalArgumentException(
                    "initial JAC cannot be negative"
            );
        }
    }

    private static void printUsage() {
        System.out.println(
                "Usage: GrpcWorkerServer "
                        + "<worker-id> <host> <grpc-port> "
                        + "[initial-jac]"
        );

        System.out.println(
                "Example: GrpcWorkerServer "
                        + "1 localhost 50051 0"
        );
    }
}