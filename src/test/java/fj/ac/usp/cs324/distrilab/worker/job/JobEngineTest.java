package fj.ac.usp.cs324.distrilab.worker.job;

import fj.ac.usp.cs324.distrilab.common.model.JobRequest;
import fj.ac.usp.cs324.distrilab.common.model.JobResult;
import fj.ac.usp.cs324.distrilab.common.model.JobType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

class JobEngineTest {

    private final JobEngine jobEngine = new JobEngine();

    @Test
    void maxReturnsLargestNumber() {
        JobRequest request = new JobRequest(
                "max-1",
                JobType.MAX,
                List.of(5L, 18L, -4L, 12L),
                0L,
                0L
        );

        JobResult result = jobEngine.executeJob(request);

        assertTrue(result.successful());
        assertEquals(18L, result.value());
        assertEquals("max-1", result.jobId());
        assertEquals(JobType.MAX, result.jobType());
    }

    @Test
    void primeSumReturnsSumOfPrimesInInclusiveRange() {
        JobRequest request = new JobRequest(
                "sum-1",
                JobType.PRIMESUM,
                List.of(),
                1L,
                10L
        );

        JobResult result = jobEngine.executeJob(request);

        assertTrue(result.successful());
        assertEquals(17L, result.value());
    }

    @Test
    void primeCountReturnsNumberOfPrimesInNumberList() {
        JobRequest request = new JobRequest(
                "count-1",
                JobType.PRIMECOUNT,
                List.of(2L, 3L, 4L, 5L, 10L, 11L),
                0L,
                0L
        );

        JobResult result = jobEngine.executeJob(request);

        assertTrue(result.successful());
        assertEquals(4L, result.value());
    }

    @Test
    void negativeRangeValuesAreNotTreatedAsPrime() {
        JobRequest request = new JobRequest(
                "sum-2",
                JobType.PRIMESUM,
                List.of(),
                -5L,
                3L
        );

        JobResult result = jobEngine.executeJob(request);

        assertTrue(result.successful());
        assertEquals(5L, result.value());
    }

    @Test
    void emptyMaxListReturnsFailureResult() {
        JobRequest request = new JobRequest(
                "max-empty",
                JobType.MAX,
                List.of(),
                0L,
                0L
        );

        JobResult result = jobEngine.executeJob(request);

        assertFalse(result.successful());
        assertEquals(0L, result.value());
        assertTrue(result.message().contains("cannot be empty"));
    }

    @Test
    void reversedRangeReturnsFailureResult() {
        JobRequest request = new JobRequest(
                "invalid-range",
                JobType.PRIMESUM,
                List.of(),
                20L,
                10L
        );

        JobResult result = jobEngine.executeJob(request);

        assertFalse(result.successful());
        assertEquals(0L, result.value());
        assertTrue(result.message().contains("Range start"));
    }

    @Test
    void nullRequestIsRejected() {
        assertThrows(
                IllegalArgumentException.class,
                () -> jobEngine.executeJob(null)
        );
    }

    @Test
    void multipleJobsCanRunConcurrently() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(3);

        try {
            Future<JobResult> maxFuture = executor.submit(() ->
                    jobEngine.executeJob(new JobRequest(
                            "concurrent-max",
                            JobType.MAX,
                            List.of(10L, 50L, 30L),
                            0L,
                            0L
                    ))
            );

            Future<JobResult> sumFuture = executor.submit(() ->
                    jobEngine.executeJob(new JobRequest(
                            "concurrent-sum",
                            JobType.PRIMESUM,
                            List.of(),
                            1L,
                            10L
                    ))
            );

            Future<JobResult> countFuture = executor.submit(() ->
                    jobEngine.executeJob(new JobRequest(
                            "concurrent-count",
                            JobType.PRIMECOUNT,
                            List.of(2L, 3L, 4L, 5L, 10L, 11L),
                            0L,
                            0L
                    ))
            );

            assertEquals(50L, maxFuture.get().value());
            assertEquals(17L, sumFuture.get().value());
            assertEquals(4L, countFuture.get().value());
        } finally {
            executor.shutdownNow();
        }
    }
}