package fj.ac.usp.cs324.distrilab.worker.job;

import fj.ac.usp.cs324.distrilab.grpc.JobRequest;
import fj.ac.usp.cs324.distrilab.grpc.JobType;
import fj.ac.usp.cs324.distrilab.grpc.TaskRequest;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class JobTaskCoordinatorTest {

    @Test
    void registerJobCreatesAndTracksPartitions() {
        JobProgressTracker tracker = new JobProgressTracker();

        JobTaskCoordinator coordinator = new JobTaskCoordinator(
                new JobTaskPartitioner(),
                tracker
        );

        JobRequest request = JobRequest.newBuilder()
                .setJobId("job-1")
                .setJobType(JobType.JOB_TYPE_MAX)
                .addAllNumbers(List.of(
                        10L, 20L, 30L, 40L, 50L, 60L
                ))
                .build();

        List<TaskRequest> tasks = coordinator.registerJob(
                request,
                List.of(1, 2, 3),
                99,
                1L
        );

        assertEquals(3, tasks.size());
        assertEquals("job-1", tasks.getFirst().getJobId());
        assertEquals(
                3,
                tracker.getSnapshot("job-1").getTasksCount()
        );
    }

    @Test
    void registeringSameJobAgainIsIdempotent() {
        JobProgressTracker tracker = new JobProgressTracker();

        JobTaskCoordinator coordinator = new JobTaskCoordinator(
                new JobTaskPartitioner(),
                tracker
        );

        JobRequest request = JobRequest.newBuilder()
                .setJobId("job-duplicate")
                .setJobType(JobType.JOB_TYPE_PRIMECOUNT)
                .addAllNumbers(List.of(2L, 3L, 4L, 5L))
                .build();

        List<TaskRequest> first = coordinator.registerJob(
                request,
                List.of(1, 2),
                99,
                1L
        );

        List<TaskRequest> second = coordinator.registerJob(
                request,
                List.of(1, 2),
                99,
                1L
        );

        assertEquals(first, second);
        assertEquals(
                2,
                tracker.getSnapshot("job-duplicate").getTasksCount()
        );
    }

    @Test
    void unfinishedTaskIsReassignedAfterWorkerFailure() {
        JobProgressTracker tracker = new JobProgressTracker();

        JobTaskCoordinator coordinator = new JobTaskCoordinator(
                new JobTaskPartitioner(),
                tracker
        );

        JobRequest request = JobRequest.newBuilder()
                .setJobId("job-reassign")
                .setJobType(JobType.JOB_TYPE_MAX)
                .addAllNumbers(List.of(10L, 20L, 30L, 40L))
                .build();

        List<TaskRequest> originalTasks = coordinator.registerJob(
                request,
                List.of(1, 2),
                99,
                3L
        );

        TaskRequest failedWorkerTask = originalTasks.stream()
                .filter(task -> task.getAssignedWorkerId() == 1)
                .findFirst()
                .orElseThrow();

        List<TaskRequest> reassigned =
                coordinator.reassignUnfinishedTasks(
                        "job-reassign",
                        1,
                        List.of(2, 3),
                        99,
                        3L
                );

        assertEquals(1, reassigned.size());

        TaskRequest replacement = reassigned.getFirst();

        assertEquals(
                failedWorkerTask.getTaskId(),
                replacement.getTaskId()
        );
        assertEquals(2, replacement.getAttempt());
        assertNotEquals(1, replacement.getAssignedWorkerId());
        assertTrue(
                List.of(2, 3).contains(
                        replacement.getAssignedWorkerId()
                )
        );
    }

    @Test
    void reassignmentFailsWhenNoReplacementWorkerExists() {
        JobProgressTracker tracker = new JobProgressTracker();

        JobTaskCoordinator coordinator = new JobTaskCoordinator(
                new JobTaskPartitioner(),
                tracker
        );

        JobRequest request = JobRequest.newBuilder()
                .setJobId("job-no-worker")
                .setJobType(JobType.JOB_TYPE_MAX)
                .addAllNumbers(List.of(10L, 20L))
                .build();

        coordinator.registerJob(
                request,
                List.of(1),
                99,
                1L
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> coordinator.reassignUnfinishedTasks(
                        "job-no-worker",
                        1,
                        List.of(),
                        99,
                        1L
                )
        );
    }
}