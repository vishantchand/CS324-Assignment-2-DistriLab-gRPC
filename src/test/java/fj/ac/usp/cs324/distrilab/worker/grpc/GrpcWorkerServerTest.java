package fj.ac.usp.cs324.distrilab.worker.grpc;

import fj.ac.usp.cs324.distrilab.bootstrap.grpc.GrpcBootstrapService;
import fj.ac.usp.cs324.distrilab.grpc.WorkerInfo;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GrpcWorkerServerTest {

    private GrpcBootstrapService bootstrapService;
    private Server bootstrapServer;

    private GrpcWorkerServer workerOne;
    private GrpcWorkerServer workerTwo;

    @BeforeEach
    void setUp() throws Exception {
        bootstrapService = new GrpcBootstrapService();

        bootstrapServer = ServerBuilder
                .forPort(0)
                .addService(bootstrapService)
                .build()
                .start();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (workerTwo != null) {
            workerTwo.close();
        }

        if (workerOne != null) {
            workerOne.close();
        }

        if (bootstrapServer != null) {
            bootstrapServer.shutdownNow();
            bootstrapServer.awaitTermination(
                    5,
                    TimeUnit.SECONDS
            );
        }
    }

    @Test
    void registersWorkerWithBootstrapAndStartsRuntime()
            throws Exception {
        workerOne = createWorker(1);

        workerOne.start();

        assertTrue(workerOne.isStarted());
        assertTrue(bootstrapService.containsWorker(1));
        assertEquals(
                1,
                bootstrapService.getActiveWorkerCount()
        );
        assertTrue(
                workerOne.getNeighboursSnapshot().isEmpty()
        );
    }

    @Test
    void dynamicallyJoinsExistingWorkerNetwork()
            throws Exception {
        workerOne = createWorker(1);
        workerTwo = createWorker(2);

        workerOne.start();
        workerTwo.start();

        awaitCondition(
                () -> containsWorker(
                        workerOne.getNeighboursSnapshot(),
                        2
                ),
                Duration.ofSeconds(5)
        );

        awaitCondition(
                () -> containsWorker(
                        workerTwo.getNeighboursSnapshot(),
                        1
                ),
                Duration.ofSeconds(5)
        );

        assertEquals(
                2,
                bootstrapService.getActiveWorkerCount()
        );

        assertTrue(
                containsWorker(
                        workerOne.getNeighboursSnapshot(),
                        2
                )
        );

        assertTrue(
                containsWorker(
                        workerTwo.getNeighboursSnapshot(),
                        1
                )
        );
    }

    @Test
    void gracefulShutdownUnregistersAndNotifiesNeighbours()
            throws Exception {
        workerOne = createWorker(1);
        workerTwo = createWorker(2);

        workerOne.start();
        workerTwo.start();

        awaitCondition(
                () -> containsWorker(
                        workerOne.getNeighboursSnapshot(),
                        2
                ),
                Duration.ofSeconds(5)
        );

        workerTwo.close();

        awaitCondition(
                () -> !bootstrapService.containsWorker(2),
                Duration.ofSeconds(5)
        );

        awaitCondition(
                () -> !containsWorker(
                        workerOne.getNeighboursSnapshot(),
                        2
                ),
                Duration.ofSeconds(5)
        );

        assertFalse(bootstrapService.containsWorker(2));
        assertTrue(bootstrapService.containsWorker(1));
        assertFalse(
                containsWorker(
                        workerOne.getNeighboursSnapshot(),
                        2
                )
        );

        workerTwo = null;
    }

    @Test
    void rejectsInvalidRuntimeConfiguration() {
        int bootstrapPort = bootstrapServer.getPort();

        assertThrows(
                IllegalArgumentException.class,
                () -> new GrpcWorkerServer(
                        0,
                        "localhost",
                        freePort(),
                        "localhost",
                        bootstrapPort
                )
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> new GrpcWorkerServer(
                        1,
                        "",
                        freePort(),
                        "localhost",
                        bootstrapPort
                )
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> new GrpcWorkerServer(
                        1,
                        "localhost",
                        0,
                        "localhost",
                        bootstrapPort
                )
        );
    }

    private GrpcWorkerServer createWorker(int workerId)
            throws Exception {
        return new GrpcWorkerServer(
                workerId,
                "localhost",
                freePort(),
                "localhost",
                bootstrapServer.getPort()
        );
    }

    private static boolean containsWorker(
            List<WorkerInfo> workers,
            int workerId
    ) {
        return workers.stream().anyMatch(
                worker -> worker.getWorkerId() == workerId
        );
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void awaitCondition(
            BooleanSupplier condition,
            Duration timeout
    ) throws InterruptedException {
        long deadline =
                System.nanoTime() + timeout.toNanos();

        while (!condition.getAsBoolean()
                && System.nanoTime() < deadline) {
            Thread.sleep(25L);
        }

        assertTrue(
                condition.getAsBoolean(),
                "Condition was not satisfied before timeout"
        );
    }
}