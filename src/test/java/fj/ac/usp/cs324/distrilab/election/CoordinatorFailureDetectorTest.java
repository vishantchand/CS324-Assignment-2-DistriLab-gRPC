package fj.ac.usp.cs324.distrilab.election;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class CoordinatorFailureDetectorTest {

    @Test
    void coordinatorIsAliveBeforeTimeout() {
        AtomicLong currentTime = new AtomicLong(1000);
        AtomicInteger electionsStarted = new AtomicInteger();

        CoordinatorFailureDetector detector =
                new CoordinatorFailureDetector(
                        5000,
                        electionsStarted::incrementAndGet,
                        currentTime::get
                );

        currentTime.set(5999);

        assertTrue(detector.checkCoordinator());
        assertFalse(detector.hasTriggeredElection());
        assertEquals(0, electionsStarted.get());
    }

    @Test
    void timeoutTriggersElection() {
        AtomicLong currentTime = new AtomicLong(1000);
        AtomicInteger electionsStarted = new AtomicInteger();

        CoordinatorFailureDetector detector =
                new CoordinatorFailureDetector(
                        5000,
                        electionsStarted::incrementAndGet,
                        currentTime::get
                );

        currentTime.set(6000);

        assertFalse(detector.checkCoordinator());
        assertTrue(detector.hasTriggeredElection());
        assertEquals(1, electionsStarted.get());
    }

    @Test
    void repeatedChecksTriggerOnlyOneElection() {
        AtomicLong currentTime = new AtomicLong(1000);
        AtomicInteger electionsStarted = new AtomicInteger();

        CoordinatorFailureDetector detector =
                new CoordinatorFailureDetector(
                        5000,
                        electionsStarted::incrementAndGet,
                        currentTime::get
                );

        currentTime.set(7000);

        detector.checkCoordinator();
        detector.checkCoordinator();
        detector.checkCoordinator();

        assertEquals(1, electionsStarted.get());
    }

    @Test
    void heartbeatResetsFailureDetection() {
        AtomicLong currentTime = new AtomicLong(1000);
        AtomicInteger electionsStarted = new AtomicInteger();

        CoordinatorFailureDetector detector =
                new CoordinatorFailureDetector(
                        5000,
                        electionsStarted::incrementAndGet,
                        currentTime::get
                );

        currentTime.set(6000);
        assertFalse(detector.checkCoordinator());

        currentTime.set(7000);
        detector.recordHeartbeat();

        currentTime.set(11000);

        assertTrue(detector.checkCoordinator());
        assertFalse(detector.hasTriggeredElection());
        assertEquals(1, electionsStarted.get());
        assertEquals(7000, detector.getLastHeartbeatTimeMs());
    }

    @Test
    void invalidTimeoutIsRejected() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new CoordinatorFailureDetector(
                        0,
                        () -> {
                        }
                )
        );
    }

    @Test
    void nullElectionStarterIsRejected() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new CoordinatorFailureDetector(
                        5000,
                        null
                )
        );
    }
}