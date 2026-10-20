package fj.ac.usp.cs324.distrilab.worker.job;

import fj.ac.usp.cs324.distrilab.grpc.JobRequest;
import fj.ac.usp.cs324.distrilab.grpc.JobType;
import fj.ac.usp.cs324.distrilab.grpc.TaskRequest;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JobTaskPartitionerTest {

    private final JobTaskPartitioner partitioner =
            new JobTaskPartitioner();

    @Test
    void partitionsNumberListEvenly() {
        JobRequest job = JobRequest.newBuilder()
                .setJobId("max-job")
                .setJobType(JobType.JOB_TYPE_MAX)
                .addAllNumbers(
                        List.of(
                                4L, 8L, 12L, 16L,
                                20L, 24L, 28L, 32L
                        )
                )
                .build();

        List<TaskRequest> tasks =
                partitioner.createTasks(
                        job,
                        List.of(10, 20, 30),
                        5L
                );

        assertEquals(3, tasks.size());

        assertEquals(3, tasks.get(0).getNumbersCount());
        assertEquals(3, tasks.get(1).getNumbersCount());
        assertEquals(2, tasks.get(2).getNumbersCount());

        assertEquals(10, tasks.get(0).getAssignedWorkerId());
        assertEquals(20, tasks.get(1).getAssignedWorkerId());
        assertEquals(30, tasks.get(2).getAssignedWorkerId());

        assertEquals("max-job", tasks.get(0).getJobId());
        assertEquals("max-job-task-1", tasks.get(0).getTaskId());
        assertEquals("max-job-task-2", tasks.get(1).getTaskId());
        assertEquals("max-job-task-3", tasks.get(2).getTaskId());

        assertEquals(1, tasks.get(0).getAttempt());
        assertEquals(5L, tasks.get(0).getCoordinatorTerm());
    }

    @Test
    void partitionsPrimeSumRangeEvenly() {
        JobRequest job = JobRequest.newBuilder()
                .setJobId("sum-job")
                .setJobType(JobType.JOB_TYPE_PRIMESUM)
                .setRangeStart(1L)
                .setRangeEnd(10L)
                .build();

        List<TaskRequest> tasks =
                partitioner.createTasks(
                        job,
                        List.of(1, 2, 3),
                        2L
                );

        assertEquals(3, tasks.size());

        assertEquals(1L, tasks.get(0).getRangeStart());
        assertEquals(4L, tasks.get(0).getRangeEnd());
        assertEquals(4L, tasks.get(0).getTotalWorkUnits());

        assertEquals(5L, tasks.get(1).getRangeStart());
        assertEquals(7L, tasks.get(1).getRangeEnd());
        assertEquals(3L, tasks.get(1).getTotalWorkUnits());

        assertEquals(8L, tasks.get(2).getRangeStart());
        assertEquals(10L, tasks.get(2).getRangeEnd());
        assertEquals(3L, tasks.get(2).getTotalWorkUnits());
    }

    @Test
    void doesNotCreateEmptyTasks() {
        JobRequest job = JobRequest.newBuilder()
                .setJobId("count-job")
                .setJobType(JobType.JOB_TYPE_PRIMECOUNT)
                .addAllNumbers(List.of(2L, 7L))
                .build();

        List<TaskRequest> tasks =
                partitioner.createTasks(
                        job,
                        List.of(1, 2, 3, 4),
                        1L
                );

        assertEquals(2, tasks.size());
        assertEquals(1, tasks.get(0).getNumbersCount());
        assertEquals(1, tasks.get(1).getNumbersCount());
    }

    @Test
    void removesDuplicateWorkerIds() {
        JobRequest job = JobRequest.newBuilder()
                .setJobId("duplicate-worker-job")
                .setJobType(JobType.JOB_TYPE_MAX)
                .addAllNumbers(
                        List.of(2L, 4L, 6L, 8L)
                )
                .build();

        List<TaskRequest> tasks =
                partitioner.createTasks(
                        job,
                        List.of(1, 1, 2),
                        1L
                );

        assertEquals(2, tasks.size());
        assertEquals(1, tasks.get(0).getAssignedWorkerId());
        assertEquals(2, tasks.get(1).getAssignedWorkerId());
    }

    @Test
    void rejectsInvalidPartitionRequests() {
        JobRequest validJob = JobRequest.newBuilder()
                .setJobId("valid-job")
                .setJobType(JobType.JOB_TYPE_MAX)
                .addNumbers(10L)
                .build();

        assertThrows(
                IllegalArgumentException.class,
                () -> partitioner.createTasks(
                        validJob,
                        List.of(),
                        1L
                )
        );

        JobRequest reversedRange =
                JobRequest.newBuilder()
                        .setJobId("invalid-range")
                        .setJobType(
                                JobType.JOB_TYPE_PRIMESUM
                        )
                        .setRangeStart(20L)
                        .setRangeEnd(10L)
                        .build();

        assertThrows(
                IllegalArgumentException.class,
                () -> partitioner.createTasks(
                        reversedRange,
                        List.of(1, 2),
                        1L
                )
        );
    }
}
