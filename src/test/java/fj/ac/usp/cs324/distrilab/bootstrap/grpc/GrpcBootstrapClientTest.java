package fj.ac.usp.cs324.distrilab.bootstrap.grpc;

import fj.ac.usp.cs324.distrilab.grpc.Acknowledgement;
import fj.ac.usp.cs324.distrilab.grpc.RegisterWorkerReply;
import fj.ac.usp.cs324.distrilab.grpc.WorkerInfo;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GrpcBootstrapClientTest {

    private Server server;
    private GrpcBootstrapClient client;

    @BeforeEach
    void setUp() throws Exception {
        GrpcBootstrapService service =
                new GrpcBootstrapService();

        server = ServerBuilder
                .forPort(0)
                .addService(service)
                .build()
                .start();

        client = new GrpcBootstrapClient(
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
            server.awaitTermination();
        }
    }

    @Test
    void registersWorkerThroughGrpc() {
        WorkerInfo worker = worker(1, 5001);

        RegisterWorkerReply reply =
                client.registerWorker(worker);

        assertTrue(reply.getSuccess());
        assertEquals(1, reply.getActiveWorkersCount());
        assertEquals(worker, reply.getActiveWorkers(0));
    }

    @Test
    void unregistersWorkerThroughGrpc() {
        WorkerInfo worker = worker(1, 5001);
        client.registerWorker(worker);

        Acknowledgement reply =
                client.unregisterWorker(worker.getWorkerId());

        assertTrue(reply.getSuccess());
        assertTrue(client.listActiveWorkers().isEmpty());
    }

    @Test
    void listsActiveWorkersThroughGrpc() {
        WorkerInfo workerOne = worker(1, 5001);
        WorkerInfo workerTwo = worker(2, 5002);

        client.registerWorker(workerTwo);
        client.registerWorker(workerOne);

        List<WorkerInfo> workers =
                client.listActiveWorkers();

        assertEquals(2, workers.size());
        assertEquals(1, workers.get(0).getWorkerId());
        assertEquals(2, workers.get(1).getWorkerId());
    }

    @Test
    void discoversRegisteredWorkerThroughGrpc() {
        WorkerInfo worker = worker(1, 5001);
        client.registerWorker(worker);

        Optional<WorkerInfo> selected =
                client.getRandomWorker();

        assertTrue(selected.isPresent());
        assertEquals(worker, selected.orElseThrow());
    }

    @Test
    void returnsEmptyWhenNoWorkerIsAvailable() {
        assertTrue(client.getRandomWorker().isEmpty());
    }

    @Test
    void closesOwnedChannel() {
        assertFalse(client.isShutdown());

        client.close();

        assertTrue(client.isShutdown());
    }

    @Test
    void rejectsInvalidConstructionArguments() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new GrpcBootstrapClient(
                        "",
                        5000
                )
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> new GrpcBootstrapClient(
                        "localhost",
                        0
                )
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> new GrpcBootstrapClient(
                        "localhost",
                        5000,
                        Duration.ZERO
                )
        );
    }

    @Test
    void rejectsInvalidWorkerId() {
        assertThrows(
                IllegalArgumentException.class,
                () -> client.unregisterWorker(0)
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
                .setWorkerName("Worker-" + workerId)
                .build();
    }
}