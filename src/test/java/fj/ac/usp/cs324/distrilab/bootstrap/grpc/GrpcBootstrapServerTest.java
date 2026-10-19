package fj.ac.usp.cs324.distrilab.bootstrap.grpc;

import fj.ac.usp.cs324.distrilab.grpc.WorkerInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GrpcBootstrapServerTest {

    private GrpcBootstrapServer server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.close();
        }
    }

    @Test
    void startsOnAvailablePort() throws Exception {
        server = new GrpcBootstrapServer(0);

        server.start();

        assertTrue(server.isRunning());
        assertTrue(server.getPort() > 0);
    }

    @Test
    void acceptsGrpcClientRequests() throws Exception {
        server = new GrpcBootstrapServer(0);
        server.start();

        try (GrpcBootstrapClient client =
                     new GrpcBootstrapClient(
                             "localhost",
                             server.getPort(),
                             Duration.ofSeconds(2)
                     )) {

            WorkerInfo worker = WorkerInfo.newBuilder()
                    .setWorkerId(1)
                    .setHost("localhost")
                    .setGrpcPort(5001)
                    .setWorkerName("Worker-1")
                    .build();

            client.registerWorker(worker);

            assertEquals(
                    1,
                    client.listActiveWorkers().size()
            );

            assertEquals(
                    worker,
                    client.getRandomWorker().orElseThrow()
            );
        }
    }

    @Test
    void stopsCleanly() throws Exception {
        server = new GrpcBootstrapServer(0);
        server.start();

        server.stop();

        assertFalse(server.isRunning());
    }

    @Test
    void rejectsStartingTwice() throws Exception {
        server = new GrpcBootstrapServer(0);
        server.start();

        assertThrows(
                IllegalStateException.class,
                server::start
        );
    }

    @Test
    void rejectsInvalidPort() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new GrpcBootstrapServer(-1)
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> new GrpcBootstrapServer(65_536)
        );
    }

    @Test
    void startsAndStopsHealthMonitorWithServer()
            throws Exception {

        server = new GrpcBootstrapServer(0);

        server.start();

        assertTrue(server.isHealthMonitorRunning());

        server.stop();

        assertFalse(server.isHealthMonitorRunning());
    }
}