package fj.ac.usp.cs324.distrilab.bootstrap.grpc;

import fj.ac.usp.cs324.distrilab.grpc.WorkerInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GrpcWorkerHealthMonitorTest {

    private GrpcBootstrapService bootstrapService;
    private GrpcWorkerHealthMonitor monitor;
    private WorkerInfo worker;

    @BeforeEach
    void setUp() {
        bootstrapService = new GrpcBootstrapService();

        worker = WorkerInfo.newBuilder()
                .setWorkerId(1)
                .setHost("localhost")
                .setGrpcPort(5001)
                .setWorkerName("Worker-1")
                .build();

        bootstrapService.registerWorkerLocally(worker);
    }

    @AfterEach
    void tearDown() {
        if (monitor != null) {
            monitor.close();
        }
    }

    @Test
    void keepsReachableWorkerRegistered() {
        monitor = monitorFor(workerInfo -> true, 2);

        monitor.runHealthCheckNow();

        assertTrue(
                bootstrapService.containsWorker(
                        worker.getWorkerId()
                )
        );

        assertEquals(
                0,
                monitor.getConsecutiveFailures(
                        worker.getWorkerId()
                )
        );
    }

    @Test
    void removesWorkerAfterFailureThreshold() {
        monitor = monitorFor(workerInfo -> false, 2);

        monitor.runHealthCheckNow();

        assertTrue(
                bootstrapService.containsWorker(
                        worker.getWorkerId()
                )
        );

        assertEquals(
                1,
                monitor.getConsecutiveFailures(
                        worker.getWorkerId()
                )
        );

        monitor.runHealthCheckNow();

        assertFalse(
                bootstrapService.containsWorker(
                        worker.getWorkerId()
                )
        );
    }

    @Test
    void successfulHeartbeatResetsFailureCount() {
        AtomicBoolean reachable = new AtomicBoolean(false);

        monitor = monitorFor(
                workerInfo -> reachable.get(),
                3
        );

        monitor.runHealthCheckNow();

        assertEquals(
                1,
                monitor.getConsecutiveFailures(
                        worker.getWorkerId()
                )
        );

        reachable.set(true);
        monitor.runHealthCheckNow();

        assertEquals(
                0,
                monitor.getConsecutiveFailures(
                        worker.getWorkerId()
                )
        );

        assertTrue(
                bootstrapService.containsWorker(
                        worker.getWorkerId()
                )
        );
    }

    @Test
    void startsAndStopsSchedulerCleanly() {
        monitor = monitorFor(workerInfo -> true, 2);

        monitor.start();
        assertTrue(monitor.isRunning());

        monitor.stop();
        assertFalse(monitor.isRunning());
    }

    @Test
    void rejectsInvalidConfiguration() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new GrpcWorkerHealthMonitor(
                        bootstrapService,
                        Duration.ZERO,
                        2,
                        workerInfo -> true
                )
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> new GrpcWorkerHealthMonitor(
                        bootstrapService,
                        Duration.ofSeconds(1),
                        0,
                        workerInfo -> true
                )
        );
    }

    private GrpcWorkerHealthMonitor monitorFor(
            GrpcWorkerHealthMonitor.WorkerPinger pinger,
            int threshold
    ) {
        return new GrpcWorkerHealthMonitor(
                bootstrapService,
                Duration.ofSeconds(1),
                threshold,
                pinger
        );
    }
}