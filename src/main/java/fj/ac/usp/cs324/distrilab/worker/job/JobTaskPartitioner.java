package fj.ac.usp.cs324.distrilab.worker.job;

import fj.ac.usp.cs324.distrilab.grpc.JobRequest;
import fj.ac.usp.cs324.distrilab.grpc.JobType;
import fj.ac.usp.cs324.distrilab.grpc.TaskRequest;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

public final class JobTaskPartitioner {

    public List<TaskRequest> createTasks(
            JobRequest job,
            List<Integer> workerIds,
            long coordinatorTerm
    ) {
        validateJob(job);

        List<Integer> availableWorkers =
                validateWorkers(workerIds);

        return switch (job.getJobType()) {
            case JOB_TYPE_MAX, JOB_TYPE_PRIMECOUNT ->
                    partitionNumbers(
                            job,
                            availableWorkers,
                            coordinatorTerm
                    );

            case JOB_TYPE_PRIMESUM ->
                    partitionRange(
                            job,
                            availableWorkers,
                            coordinatorTerm
                    );

            default -> throw new IllegalArgumentException(
                    "A valid job type is required"
            );
        };
    }

    private List<TaskRequest> partitionNumbers(
            JobRequest job,
            List<Integer> workerIds,
            long coordinatorTerm
    ) {
        List<Long> numbers = job.getNumbersList();

        if (numbers.isEmpty()) {
            throw new IllegalArgumentException(
                    "Number list cannot be empty for "
                            + job.getJobType()
            );
        }

        int partitionCount = Math.min(
                numbers.size(),
                workerIds.size()
        );

        int basicPartitionSize =
                numbers.size() / partitionCount;

        int remainingNumbers =
                numbers.size() % partitionCount;

        List<TaskRequest> tasks = new ArrayList<>();

        int startingPosition = 0;

        for (int index = 0;
             index < partitionCount;
             index++) {

            int partitionSize =
                    basicPartitionSize
                            + (index < remainingNumbers ? 1 : 0);

            int endingPosition =
                    startingPosition + partitionSize;

            List<Long> partitionNumbers =
                    List.copyOf(
                            numbers.subList(
                                    startingPosition,
                                    endingPosition
                            )
                    );

            tasks.add(
                    TaskRequest.newBuilder()
                            .setJobId(job.getJobId())
                            .setTaskId(
                                    createTaskId(
                                            job.getJobId(),
                                            index
                                    )
                            )
                            .setJobType(job.getJobType())
                            .addAllNumbers(partitionNumbers)
                            .setAssignedWorkerId(
                                    workerIds.get(
                                            index
                                                    % workerIds.size()
                                    )
                            )
                            .setAttempt(1)
                            .setCoordinatorTerm(coordinatorTerm)
                            .setTotalWorkUnits(
                                    partitionNumbers.size()
                            )
                            .build()
            );

            startingPosition = endingPosition;
        }

        return List.copyOf(tasks);
    }

    private List<TaskRequest> partitionRange(
            JobRequest job,
            List<Integer> workerIds,
            long coordinatorTerm
    ) {
        if (job.getRangeStart() > job.getRangeEnd()) {
            throw new IllegalArgumentException(
                    "Range start cannot be greater than range end"
            );
        }

        BigInteger rangeStart =
                BigInteger.valueOf(job.getRangeStart());

        BigInteger rangeEnd =
                BigInteger.valueOf(job.getRangeEnd());

        BigInteger totalValues =
                rangeEnd.subtract(rangeStart)
                        .add(BigInteger.ONE);

        int partitionCount;

        if (totalValues.compareTo(
                BigInteger.valueOf(workerIds.size())
        ) < 0) {
            partitionCount = totalValues.intValueExact();
        } else {
            partitionCount = workerIds.size();
        }

        BigInteger[] division =
                totalValues.divideAndRemainder(
                        BigInteger.valueOf(partitionCount)
                );

        BigInteger basicPartitionSize = division[0];
        int remainingValues = division[1].intValueExact();

        List<TaskRequest> tasks = new ArrayList<>();

        BigInteger currentStart = rangeStart;

        for (int index = 0;
             index < partitionCount;
             index++) {

            BigInteger partitionSize =
                    basicPartitionSize.add(
                            index < remainingValues
                                    ? BigInteger.ONE
                                    : BigInteger.ZERO
                    );

            BigInteger currentEnd =
                    currentStart.add(partitionSize)
                            .subtract(BigInteger.ONE);

            long totalWorkUnits;

            try {
                totalWorkUnits =
                        partitionSize.longValueExact();
            } catch (ArithmeticException exception) {
                throw new IllegalArgumentException(
                        "Range partition is too large to track",
                        exception
                );
            }

            tasks.add(
                    TaskRequest.newBuilder()
                            .setJobId(job.getJobId())
                            .setTaskId(
                                    createTaskId(
                                            job.getJobId(),
                                            index
                                    )
                            )
                            .setJobType(JobType.JOB_TYPE_PRIMESUM)
                            .setRangeStart(
                                    currentStart.longValueExact()
                            )
                            .setRangeEnd(
                                    currentEnd.longValueExact()
                            )
                            .setAssignedWorkerId(
                                    workerIds.get(
                                            index
                                                    % workerIds.size()
                                    )
                            )
                            .setAttempt(1)
                            .setCoordinatorTerm(coordinatorTerm)
                            .setTotalWorkUnits(totalWorkUnits)
                            .build()
            );

            currentStart = currentEnd.add(BigInteger.ONE);
        }

        return List.copyOf(tasks);
    }

    private void validateJob(JobRequest job) {
        Objects.requireNonNull(
                job,
                "Job request cannot be null"
        );

        if (job.getJobId().isBlank()) {
            throw new IllegalArgumentException(
                    "Job ID cannot be blank"
            );
        }
    }

    private List<Integer> validateWorkers(
            List<Integer> workerIds
    ) {
        Objects.requireNonNull(
                workerIds,
                "Worker ID list cannot be null"
        );

        LinkedHashSet<Integer> uniqueWorkers =
                new LinkedHashSet<>();

        for (Integer workerId : workerIds) {
            if (workerId == null || workerId < 1) {
                throw new IllegalArgumentException(
                        "Worker IDs must be positive"
                );
            }

            uniqueWorkers.add(workerId);
        }

        if (uniqueWorkers.isEmpty()) {
            throw new IllegalArgumentException(
                    "At least one worker is required"
            );
        }

        return List.copyOf(uniqueWorkers);
    }

    private String createTaskId(
            String jobId,
            int taskIndex
    ) {
        return jobId + "-task-" + (taskIndex + 1);
    }
}
