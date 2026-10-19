package fj.ac.usp.cs324.distrilab.worker.grpc;

import fj.ac.usp.cs324.distrilab.grpc.Acknowledgement;
import fj.ac.usp.cs324.distrilab.grpc.HeartbeatReply;
import fj.ac.usp.cs324.distrilab.grpc.JoinNetworkReply;
import fj.ac.usp.cs324.distrilab.grpc.WorkerInfo;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GrpcWorkerClientTest {

    private Server server;
    private GrpcWorkerClient client;
    private WorkerMembershipManager membershipManager;
    private WorkerInfo localWorker;

    @BeforeEach
    void setUp() throws Exception {
        localWorker = worker(1, 5001);
        membershipManager = new WorkerMembershipManager(1);

        GrpcWorkerService service = new GrpcWorkerService(
                localWorker,
                membershipManager,
                () -> 3,
                () -> 7L
        );

        server = ServerBuilder
                .forPort(0)
                .addService(service)
                .build()
                .start();

        client = new GrpcWorkerClient(
                "localhost",
                server.getPort(),
                Duration.ofSeconds(2)
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }

        if (server != null) {
            server.shutdownNow();
            server.awaitTermination(
                    2,
                    TimeUnit.SECONDS
            );
        }
    }

    @Test
    void retrievesWorkerInformation() {
        WorkerInfo result = client.getWorkerInfo();

        assertEquals(localWorker, result);
    }

    @Test
    void sendsHeartbeatWithDeadline() {
        HeartbeatReply reply = client.ping(
                2,
                3,
                7L
        );

        assertTrue(reply.getAlive());
        assertEquals(1, reply.getWorker().getWorkerId());
        assertEquals(3, reply.getCoordinatorId());
        assertEquals(7L, reply.getCoordinatorTerm());
    }

    @Test
    void managesRemoteNeighbors() {
        WorkerInfo workerTwo = worker(2, 5002);

        Acknowledgement addReply =
                client.addNeighbor(workerTwo);

        assertTrue(addReply.getSuccess());

        List<WorkerInfo> neighbors =
                client.listNeighbors();

        assertEquals(1, neighbors.size());
        assertEquals(2, neighbors.getFirst().getWorkerId());

        Acknowledgement removeReply =
                client.removeNeighbor(2);

        assertTrue(removeReply.getSuccess());
        assertFalse(
                membershipManager.containsNeighbour(2)
        );
    }

    @Test
    void joinsRemoteWorkerNetwork() {
        WorkerInfo workerTwo = worker(2, 5002);

        JoinNetworkReply reply =
                client.joinNetwork(workerTwo);

        assertTrue(reply.getSuccess());
        assertTrue(
                membershipManager.containsNeighbour(2)
        );
        assertEquals(3, reply.getCoordinatorId());
        assertEquals(7L, reply.getCoordinatorTerm());
    }

    @Test
    void reportsRemoteWorkerFailure() {
        WorkerInfo workerTwo = worker(2, 5002);
        membershipManager.addNeighbour(workerTwo);

        Acknowledgement reply =
                client.reportWorkerFailure(
                        1,
                        2,
                        "Heartbeat deadline exceeded"
                );

        assertTrue(reply.getSuccess());
        assertFalse(
                membershipManager.containsNeighbour(2)
        );
    }

    @Test
    void rejectsInvalidEndpoint() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new GrpcWorkerClient(
                        "",
                        5001
                )
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> new GrpcWorkerClient(
                        "localhost",
                        0
                )
        );
    }

    private static WorkerInfo worker(
            int workerId,
            int port
    ) {
        return WorkerInfo.newBuilder()
                .setWorkerId(workerId)
                .setHost("localhost")
                .setGrpcPort(port)
                .setWorkerName(
                        "Worker-" + workerId
                )
                .build();
    }
}