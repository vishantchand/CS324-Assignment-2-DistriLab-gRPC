package fj.ac.usp.cs324.distrilab.worker.job;

import fj.ac.usp.cs324.distrilab.common.model.JobRequest;
import fj.ac.usp.cs324.distrilab.common.model.JobResult;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.function.Consumer;

public class JobEngine {

    public record CalculationProgress(
            long completedWorkUnits,
            long totalWorkUnits,
            long partialValue
    ) {
    }

    // Keeps existing callers and A1 tests working.
    public JobResult executeJob(JobRequest request) {
        return executeJob(request, progress -> {
        });
    }

    public JobResult executeJob(
            JobRequest request,
            Consumer<CalculationProgress> progressListener
    ) {
        if (request == null) {
            throw new IllegalArgumentException(
                    "Job request cannot be null"
            );
        }

        Objects.requireNonNull(
                progressListener,
                "Progress listener cannot be null"
        );

        try {
            long value = switch (request.jobType()) {
                case MAX -> executeMax(request, progressListener);
                case PRIMESUM -> executePrimeSum(
                        request,
                        progressListener
                );
                case PRIMECOUNT -> executePrimeCount(
                        request,
                        progressListener
                );
            };

            return new JobResult(
                    request.jobId(),
                    request.jobType(),
                    value,
                    true,
                    "Job completed successfully"
            );
        } catch (IllegalArgumentException | ArithmeticException exception) {
            return new JobResult(
                    request.jobId(),
                    request.jobType(),
                    0L,
                    false,
                    exception.getMessage()
            );
        }
    }

    private long executeMax(
            JobRequest request,
            Consumer<CalculationProgress> listener
    ) {
        List<Long> numbers = request.numbers();

        if (numbers.isEmpty()) {
            throw new IllegalArgumentException(
                    "Number list cannot be empty for a MAX job"
            );
        }

        long total = numbers.size();
        long completed = 0L;
        long maximum = Long.MIN_VALUE;

        reportProgress(listener, completed, total, maximum);

        for (long number : numbers) {
            checkInterrupted();

            maximum = Math.max(maximum, number);
            completed++;

            reportProgress(listener, completed, total, maximum);
        }

        return maximum;
    }

    private long executePrimeSum(
            JobRequest request,
            Consumer<CalculationProgress> listener
    ) {
        long start = request.rangeStart();
        long end = request.rangeEnd();

        if (start > end) {
            throw new IllegalArgumentException(
                    "Range start cannot be greater than range end"
            );
        }

        // Rejects ranges whose work-unit count cannot fit in a long.
        long total = Math.addExact(
                Math.subtractExact(end, start),
                1L
        );

        long completed = 0L;
        long sum = 0L;

        reportProgress(listener, completed, total, sum);

        for (long number = start; ; number++) {
            checkInterrupted();

            if (isPrime(number)) {
                sum = Math.addExact(sum, number);
            }

            completed++;
            reportProgress(listener, completed, total, sum);

            // Also prevents overflow when end is Long.MAX_VALUE.
            if (number == end) {
                break;
            }
        }

        return sum;
    }

    private long executePrimeCount(
            JobRequest request,
            Consumer<CalculationProgress> listener
    ) {
        List<Long> numbers = request.numbers();

        if (numbers.isEmpty()) {
            throw new IllegalArgumentException(
                    "Number list cannot be empty for a PRIMECOUNT job"
            );
        }

        long total = numbers.size();
        long completed = 0L;
        long count = 0L;

        reportProgress(listener, completed, total, count);

        for (long number : numbers) {
            checkInterrupted();

            if (isPrime(number)) {
                count++;
            }

            completed++;
            reportProgress(listener, completed, total, count);
        }

        return count;
    }

    private void reportProgress(
            Consumer<CalculationProgress> listener,
            long completed,
            long total,
            long partialValue
    ) {
        // Limits update frequency for large inputs.
        long interval = Math.max(1L, total / 100L);

        if (completed == 0L
                || completed == total
                || completed % interval == 0L) {

            listener.accept(new CalculationProgress(
                    completed,
                    total,
                    partialValue
            ));
        }
    }

    private void checkInterrupted() {
        if (Thread.currentThread().isInterrupted()) {
            throw new CancellationException(
                    "Job execution was interrupted"
            );
        }
    }

    private boolean isPrime(long number) {
        if (number < 2L) {
            return false;
        }

        if (number == 2L) {
            return true;
        }

        if (number % 2L == 0L) {
            return false;
        }

        for (long divisor = 3L;
             divisor <= number / divisor;
             divisor += 2L) {

            checkInterrupted();

            if (number % divisor == 0L) {
                return false;
            }
        }

        return true;
    }
}