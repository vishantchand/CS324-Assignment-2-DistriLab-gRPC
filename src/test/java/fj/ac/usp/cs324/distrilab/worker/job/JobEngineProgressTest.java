package fj.ac.usp.cs324.distrilab.worker.job;

import fj.ac.usp.cs324.distrilab.common.model.JobRequest;
import fj.ac.usp.cs324.distrilab.common.model.JobResult;
import fj.ac.usp.cs324.distrilab.common.model.JobType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;

import static org.junit.jupiter.api.Assertions.*;

class JobEngineProgressTest {

    private final JobEngine engine = new JobEngine();

    @Test
    void maxReportsProgressAndCorrectPartialValues() {
        var updates = new ArrayList<JobEngine.CalculationProgress>();

        JobResult result = engine.executeJob(
                new JobRequest(
                        "max-progress",
                        JobType.MAX,
                        List.of(-8L, -3L, -10L),
                        0L,
                        0L
                ),
                updates::add
        );

        assertTrue(result.successful());
        assertEquals(-3L, result.value());

        assertEquals(
                List.of(0L, 1L, 2L, 3L),
                updates.stream()
                        .map(JobEngine.CalculationProgress::completedWorkUnits)
                        .toList()
        );

        assertEquals(-8L, updates.get(1).partialValue());
        assertEquals(-3L, updates.get(2).partialValue());
        assertEquals(-3L, updates.get(3).partialValue());
        assertTrue(updates.stream()
                .allMatch(update -> update.totalWorkUnits() == 3L));
    }

    @Test
    void primeSumReportsActualCompletedNumbers() {
        var updates = new ArrayList<JobEngine.CalculationProgress>();

        JobResult result = engine.executeJob(
                new JobRequest(
                        "sum-progress",
                        JobType.PRIMESUM,
                        List.of(),
                        1L,
                        10L
                ),
                updates::add
        );

        assertTrue(result.successful());
        assertEquals(17L, result.value());

        assertEquals(0L, updates.getFirst().completedWorkUnits());
        assertEquals(10L, updates.getLast().completedWorkUnits());
        assertEquals(10L, updates.getLast().totalWorkUnits());
        assertEquals(17L, updates.getLast().partialValue());

        // After checking 1, 2 and 3, the prime sum is 5.
        assertEquals(3L, updates.get(3).completedWorkUnits());
        assertEquals(5L, updates.get(3).partialValue());
    }

    @Test
    void primeCountUsesListAndCountsDuplicatePrimes() {
        var updates = new ArrayList<JobEngine.CalculationProgress>();

        JobResult result = engine.executeJob(
                new JobRequest(
                        "count-progress",
                        JobType.PRIMECOUNT,
                        List.of(2L, 2L, 4L, 5L, -3L),
                        100L,
                        200L
                ),
                updates::add
        );

        assertTrue(result.successful());
        assertEquals(3L, result.value());
        assertEquals(5L, updates.getLast().completedWorkUnits());
        assertEquals(5L, updates.getLast().totalWorkUnits());
        assertEquals(3L, updates.getLast().partialValue());
    }

    @Test
    void invalidInputDoesNotReportCompletedWork() {
        var updates = new ArrayList<JobEngine.CalculationProgress>();

        JobResult result = engine.executeJob(
                new JobRequest(
                        "empty-max",
                        JobType.MAX,
                        List.of(),
                        0L,
                        0L
                ),
                updates::add
        );

        assertFalse(result.successful());
        assertTrue(updates.isEmpty());
    }

    @Test
    void oversizedRangeReturnsFailureWithoutStartingCalculation() {
        var updates = new ArrayList<JobEngine.CalculationProgress>();

        JobResult result = engine.executeJob(
                new JobRequest(
                        "oversized-range",
                        JobType.PRIMESUM,
                        List.of(),
                        Long.MIN_VALUE,
                        Long.MAX_VALUE
                ),
                updates::add
        );

        assertFalse(result.successful());
        assertTrue(updates.isEmpty());
    }

    @Test
    void interruptedCalculationStopsWithoutReportingCompletion() {
        var updates = new ArrayList<JobEngine.CalculationProgress>();

        try {
            Thread.currentThread().interrupt();

            assertThrows(
                    CancellationException.class,
                    () -> engine.executeJob(
                            new JobRequest(
                                    "cancelled-job",
                                    JobType.MAX,
                                    List.of(1L, 2L, 3L),
                                    0L,
                                    0L
                            ),
                            updates::add
                    )
            );

            assertTrue(updates.stream()
                    .noneMatch(update ->
                            update.completedWorkUnits()
                                    == update.totalWorkUnits()));
        } finally {
            // Clear the flag so other tests are unaffected.
            Thread.interrupted();
        }
    }
}
