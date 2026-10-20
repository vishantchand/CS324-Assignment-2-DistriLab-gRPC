package fj.ac.usp.cs324.distrilab.worker;

import fj.ac.usp.cs324.distrilab.grpc.CoordinatorStateSnapshot;
import fj.ac.usp.cs324.distrilab.grpc.ElectionServiceGrpc;
import fj.ac.usp.cs324.distrilab.grpc.Empty;
import fj.ac.usp.cs324.distrilab.grpc.HeartbeatReply;
import fj.ac.usp.cs324.distrilab.grpc.HeartbeatRequest;
import fj.ac.usp.cs324.distrilab.grpc.RecoveryServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class GrpcWorkerServerTest {

    @Test
    void electionAndRecoveryServicesAreAvailable()
            throws Exception {
        int port = findAvailablePort();

        GrpcWorkerServer workerServer =
                new GrpcWorkerServer(
                        4,
                        "localhost",
                        port,
                        2
                );

        ManagedChannel channel = null;

        try {
            workerServer.start();

            channel = ManagedChannelBuilder
                    .forAddress(
                            "localhost",
                            workerServer.getPort()
                    )
                    .usePlaintext()
                    .build();

            ElectionServiceGrpc.ElectionServiceBlockingStub
                    electionStub =
                    ElectionServiceGrpc.newBlockingStub(channel);

            HeartbeatReply heartbeat =
                    electionStub.coordinatorHeartbeat(
                            HeartbeatRequest.newBuilder()
                                    .setSenderId(8)
                                    .setKnownCoordinatorId(-1)
                                    .setCoordinatorTerm(0)
                                    .setTimestampMs(
                                            System.currentTimeMillis()
                                    )
                                    .build()
                    );

            assertFalse(heartbeat.getAlive());
            assertEquals(
                    4,
                    heartbeat.getWorker().getWorkerId()
            );
            assertEquals(
                    port,
                    heartbeat.getWorker().getGrpcPort()
            );

            RecoveryServiceGrpc.RecoveryServiceBlockingStub
                    recoveryStub =
                    RecoveryServiceGrpc.newBlockingStub(channel);

            CoordinatorStateSnapshot snapshot =
                    recoveryStub.getCoordinatorState(
                            Empty.getDefaultInstance()
                    );

            assertEquals(-1, snapshot.getCoordinatorId());
            assertEquals(0, snapshot.getCoordinatorTerm());
            assertEquals(0, snapshot.getVersion());
        } finally {
            if (channel != null) {
                channel.shutdownNow();
                channel.awaitTermination(
                        5,
                        TimeUnit.SECONDS
                );
            }

            workerServer.stop();
        }

        assertTrue(workerServer.isShutdown());
    }

    @Test
    void invalidServerConfigurationIsRejected() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new GrpcWorkerServer(
                        -1,
                        "localhost",
                        50051
                )
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> new GrpcWorkerServer(
                        1,
                        " ",
                        50051
                )
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> new GrpcWorkerServer(
                        1,
                        "localhost",
                        70000
                )
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> new GrpcWorkerServer(
                        1,
                        "localhost",
                        50051,
                        -1
                )
        );
    }

    private int findAvailablePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}