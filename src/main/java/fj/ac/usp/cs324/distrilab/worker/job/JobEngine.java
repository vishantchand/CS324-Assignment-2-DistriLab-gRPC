package fj.ac.usp.cs324.distrilab.worker.job;

import fj.ac.usp.cs324.distrilab.common.model.JobRequest;
import fj.ac.usp.cs324.distrilab.common.model.JobResult;

import java.util.List;

public class JobEngine {

    public JobResult executeJob(JobRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("Job request cannot be null");
        }

        try {
            long value = switch (request.jobType()) {
                case MAX -> executeMax(request);
                case PRIMESUM -> executePrimeSum(request);
                case PRIMECOUNT -> executePrimeCount(request);
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

    private long executeMax(JobRequest request) {
        List<Long> numbers = request.numbers();

        if (numbers.isEmpty()) {
            throw new IllegalArgumentException(
                    "Number list cannot be empty for a MAX job"
            );
        }

        long maximum = numbers.getFirst();

        for (long number : numbers) {
            if (number > maximum) {
                maximum = number;
            }
        }

        return maximum;
    }

    private long executePrimeSum(JobRequest request) {
        validateRange(request.rangeStart(), request.rangeEnd());

        long sum = 0L;

        for (long number = request.rangeStart();
             number <= request.rangeEnd();
             number++) {

            if (isPrime(number)) {
                sum = Math.addExact(sum, number);
            }

            if (number == Long.MAX_VALUE) {
                break;
            }
        }

        return sum;
    }

    private long executePrimeCount(JobRequest request) {
        List<Long> numbers = request.numbers();

        if (numbers.isEmpty()) {
            throw new IllegalArgumentException(
                    "Number list cannot be empty for a PRIMECOUNT job"
            );
        }

        long count = 0L;

        for (long number : numbers) {
            if (isPrime(number)) {
                count++;
            }
        }

        return count;
    }

    private void validateRange(long rangeStart, long rangeEnd) {
        if (rangeStart > rangeEnd) {
            throw new IllegalArgumentException(
                    "Range start cannot be greater than range end"
            );
        }
    }

    private boolean isPrime(long number) {
        if (number < 2) {
            return false;
        }

        if (number == 2) {
            return true;
        }

        if (number % 2 == 0) {
            return false;
        }

        for (long divisor = 3;
             divisor <= number / divisor;
             divisor += 2) {

            if (number % divisor == 0) {
                return false;
            }
        }

        return true;
    }
}