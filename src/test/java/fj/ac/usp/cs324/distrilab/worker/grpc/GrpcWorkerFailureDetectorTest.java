package fj.ac.usp.cs324.distrilab.worker.grpc;

import fj.ac.usp.cs324.distrilab.grpc.WorkerHealth;
import fj.ac.usp.cs324.distrilab.grpc.WorkerInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GrpcWorkerFailureDetectorTest {

    private WorkerMembershipManager membershipManager;
    private GrpcWorkerFailureDetector detector;
    private WorkerInfo workerTwo;

    @BeforeEach
    void setUp() {
        membershipManager = new WorkerMembershipManager(1);

        workerTwo = worker(2, 5002);

        membershipManager.addNeighbour(workerTwo);
    }

    @AfterEach
    void tearDown() {
        if (detector != null) {
            detector.close();
        }
    }

    @Test
    void marksRespondingWorkerActive() {
        detector = detectorFor(
                (worker, sender, coordinator, term) -> true,
                (recipient, reporter, failed, reason) -> {
                }
        );

        detector.runHealthCheckNow();

        assertEquals(
                WorkerHealth.WORKER_HEALTH_ACTIVE,
                detector.getWorkerHealth(2)
        );

        assertEquals(
                0,
                detector.getMissedHeartbeatCount(2)
        );

        assertTrue(membershipManager.containsNeighbour(2));
    }

    @Test
    void marksWorkerSuspectedAfterMissedHeartbeat() {
        detector = detectorFor(
                (worker, sender, coordinator, term) -> false,
                (recipient, reporter, failed, reason) -> {
                }
        );

        detector.runHealthCheckNow();

        assertEquals(
                WorkerHealth.WORKER_HEALTH_SUSPECTED,
                detector.getWorkerHealth(2)
        );

        assertEquals(
                1,
                detector.getMissedHeartbeatCount(2)
        );

        assertTrue(membershipManager.containsNeighbour(2));
    }

    @Test
    void removesWorkerAfterFailureThreshold() {
        AtomicInteger reports = new AtomicInteger();

        membershipManager.addNeighbour(worker(3, 5003));

        detector = detectorFor(
                (worker, sender, coordinator, term) ->
                        worker.getWorkerId() == 3,
                (recipient, reporter, failed, reason) -> {
                    assertEquals(3, recipient.getWorkerId());
                    assertEquals(1, reporter);
                    assertEquals(2, failed);
                    reports.incrementAndGet();
                }
        );

        detector.runHealthCheckNow();
        detector.runHealthCheckNow();
        detector.runHealthCheckNow();

        assertEquals(
                WorkerHealth.WORKER_HEALTH_FAILED,
                detector.getWorkerHealth(2)
        );

        assertFalse(membershipManager.containsNeighbour(2));
        assertTrue(membershipManager.containsNeighbour(3));
        assertEquals(1, reports.get());
    }

    @Test
    void successfulHeartbeatRecoversSuspectedWorker() {
        AtomicBoolean reachable = new AtomicBoolean(false);

        detector = detectorFor(
                (worker, sender, coordinator, term) ->
                        reachable.get(),
                (recipient, reporter, failed, reason) -> {
                }
        );

        detector.runHealthCheckNow();

        assertEquals(
                WorkerHealth.WORKER_HEALTH_SUSPECTED,
                detector.getWorkerHealth(2)
        );

        reachable.set(true);
        detector.runHealthCheckNow();

        assertEquals(
                WorkerHealth.WORKER_HEALTH_ACTIVE,
                detector.getWorkerHealth(2)
        );

        assertEquals(
                0,
                detector.getMissedHeartbeatCount(2)
        );
    }

    @Test
    void startsAndStopsScheduler() {
        detector = detectorFor(
                (worker, sender, coordinator, term) -> true,
                (recipient, reporter, failed, reason) -> {
                }
        );

        detector.start();
        assertTrue(detector.isRunning());

        detector.stop();
        assertFalse(detector.isRunning());
    }

    @Test
    void rejectsInvalidConfiguration() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new GrpcWorkerFailureDetector(
                        0,
                        membershipManager,
                        () -> 1,
                        () -> 1L
                )
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> new GrpcWorkerFailureDetector(
                        1,
                        membershipManager,
                        () -> 1,
                        () -> 1L,
                        Duration.ZERO,
                        3,
                        (worker, sender, coordinator, term) -> true,
                        (recipient, reporter, failed, reason) -> {
                        }
                )
        );
    }

    private GrpcWorkerFailureDetector detectorFor(
            GrpcWorkerFailureDetector.WorkerProbe probe,
            GrpcWorkerFailureDetector.FailureReporter reporter
    ) {
        return new GrpcWorkerFailureDetector(
                1,
                membershipManager,
                () -> 3,
                () -> 7L,
                Duration.ofSeconds(1),
                3,
                probe,
                reporter
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