package fj.ac.usp.cs324.distrilab.worker.job;

import fj.ac.usp.cs324.distrilab.grpc.JobProgress;
import fj.ac.usp.cs324.distrilab.grpc.JobRequest;
import fj.ac.usp.cs324.distrilab.grpc.JobState;
import fj.ac.usp.cs324.distrilab.grpc.JobType;
import fj.ac.usp.cs324.distrilab.grpc.TaskProgress;
import fj.ac.usp.cs324.distrilab.grpc.TaskRequest;
import fj.ac.usp.cs324.distrilab.grpc.TaskState;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobProgressTrackerTest {

    @Test
    void combinesPrimeSumTaskResults() {
        JobProgressTracker tracker = new JobProgressTracker();

        JobRequest job = createJob(
                "sum-job",
                JobType.JOB_TYPE_PRIMESUM
        );

        TaskRequest firstTask = createTask(
                "sum-job",
                "task-1",
                JobType.JOB_TYPE_PRIMESUM,
                1,
                1,
                5
        );

        TaskRequest secondTask = createTask(
                "sum-job",
                "task-2",
                JobType.JOB_TYPE_PRIMESUM,
                2,
                1,
                5
        );

        assertTrue(
                tracker.registerJob(
                        job,
                        List.of(firstTask, secondTask),
                        1,
                        1
                )
        );

        tracker.recordTaskProgress(
                finalProgress(firstTask, 5L)
        );

        tracker.recordTaskProgress(
                finalProgress(secondTask, 12L)
        );

        JobProgress progress = tracker.getProgress("sum-job");

        assertEquals(
                JobState.JOB_STATE_COMPLETED,
                progress.getState()
        );

        assertEquals(100.0, progress.getOverallPercentage());
        assertTrue(progress.hasResult());
        assertTrue(progress.getResult().getSuccessful());
        assertEquals(17L, progress.getResult().getValue());
    }

    @Test
    void combinesMaxTaskResultsUsingLargestValue() {
        JobProgressTracker tracker = new JobProgressTracker();

        JobRequest job = createJob(
                "max-job",
                JobType.JOB_TYPE_MAX
        );

        TaskRequest firstTask = createTask(
                "max-job",
                "task-1",
                JobType.JOB_TYPE_MAX,
                1,
                1,
                3
        );

        TaskRequest secondTask = createTask(
                "max-job",
                "task-2",
                JobType.JOB_TYPE_MAX,
                2,
                1,
                3
        );

        tracker.registerJob(
                job,
                List.of(firstTask, secondTask),
                1,
                1
        );

        tracker.recordTaskProgress(
                finalProgress(firstTask, 15L)
        );

        tracker.recordTaskProgress(
                finalProgress(secondTask, 42L)
        );

        JobProgress progress = tracker.getProgress("max-job");

        assertEquals(
                JobState.JOB_STATE_COMPLETED,
                progress.getState()
        );

        assertEquals(42L, progress.getResult().getValue());
    }

    @Test
    void calculatesOverallProgressFromAllTasks() {
        JobProgressTracker tracker = new JobProgressTracker();

        JobRequest job = createJob(
                "progress-job",
                JobType.JOB_TYPE_PRIMECOUNT
        );

        TaskRequest firstTask = createTask(
                "progress-job",
                "task-1",
                JobType.JOB_TYPE_PRIMECOUNT,
                1,
                1,
                10
        );

        TaskRequest secondTask = createTask(
                "progress-job",
                "task-2",
                JobType.JOB_TYPE_PRIMECOUNT,
                2,
                1,
                10
        );

        tracker.registerJob(
                job,
                List.of(firstTask, secondTask),
                1,
                1
        );

        tracker.recordTaskProgress(
                progressUpdate(firstTask, 10L, 4L)
        );

        tracker.recordTaskProgress(
                progressUpdate(secondTask, 5L, 2L)
        );

        JobProgress progress =
                tracker.getProgress("progress-job");

        assertEquals(
                JobState.JOB_STATE_RUNNING,
                progress.getState()
        );

        assertEquals(15L, progress.getCompletedWorkUnits());
        assertEquals(20L, progress.getTotalWorkUnits());
        assertEquals(75.0, progress.getOverallPercentage());
        assertEquals(2, progress.getWorkerProgressCount());
    }

    @Test
    void ignoresOldAttemptAfterTaskReassignment() {
        JobProgressTracker tracker = new JobProgressTracker();

        JobRequest job = createJob(
                "reassign-job",
                JobType.JOB_TYPE_PRIMESUM
        );

        TaskRequest originalTask = createTask(
                "reassign-job",
                "task-1",
                JobType.JOB_TYPE_PRIMESUM,
                1,
                1,
                10
        );

        tracker.registerJob(
                job,
                List.of(originalTask),
                1,
                1
        );

        tracker.reassignTask(
                "reassign-job",
                "task-1",
                2,
                2,
                2,
                2
        );

        boolean accepted = tracker.recordTaskProgress(
                finalProgress(originalTask, 17L)
        );

        assertFalse(accepted);

        assertEquals(
                2,
                tracker.getSnapshot("reassign-job")
                        .getTasks(0)
                        .getAttempt()
        );

        assertEquals(
                2,
                tracker.getSnapshot("reassign-job")
                        .getTasks(0)
                        .getAssignedWorkerId()
        );

        assertEquals(
                JobState.JOB_STATE_RECOVERING,
                tracker.getProgress("reassign-job").getState()
        );
    }

    @Test
    void sendsUpdatesToProgressSubscribers() throws Exception {
        JobProgressTracker tracker = new JobProgressTracker();

        JobRequest job = createJob(
                "stream-job",
                JobType.JOB_TYPE_PRIMESUM
        );

        TaskRequest task = createTask(
                "stream-job",
                "task-1",
                JobType.JOB_TYPE_PRIMESUM,
                1,
                1,
                10
        );

        tracker.registerJob(
                job,
                List.of(task),
                1,
                1
        );

        List<JobProgress> receivedUpdates =
                new ArrayList<>();

        try (AutoCloseable ignored = tracker.subscribe(
                "stream-job",
                receivedUpdates::add
        )) {
            tracker.recordTaskProgress(
                    progressUpdate(task, 5L, 10L)
            );

            tracker.recordTaskProgress(
                    finalProgress(task, 17L)
            );
        }

        assertEquals(3, receivedUpdates.size());

        assertEquals(
                JobState.JOB_STATE_ACCEPTED,
                receivedUpdates.getFirst().getState()
        );

        assertEquals(
                JobState.JOB_STATE_COMPLETED,
                receivedUpdates.getLast().getState()
        );
    }

    private JobRequest createJob(
            String jobId,
            JobType jobType
    ) {
        return JobRequest.newBuilder()
                .setJobId(jobId)
                .setJobType(jobType)
                .setRangeStart(1L)
                .setRangeEnd(10L)
                .build();
    }

    private TaskRequest createTask(
            String jobId,
            String taskId,
            JobType jobType,
            int workerId,
            int attempt,
            long totalWorkUnits
    ) {
        return TaskRequest.newBuilder()
                .setJobId(jobId)
                .setTaskId(taskId)
                .setJobType(jobType)
                .setAssignedWorkerId(workerId)
                .setAttempt(attempt)
                .setCoordinatorTerm(1L)
                .setTotalWorkUnits(totalWorkUnits)
                .build();
    }

    private TaskProgress progressUpdate(
            TaskRequest task,
            long completedWorkUnits,
            long partialValue
    ) {
        return TaskProgress.newBuilder()
                .setJobId(task.getJobId())
                .setTaskId(task.getTaskId())
                .setWorkerId(task.getAssignedWorkerId())
                .setCompletedWorkUnits(completedWorkUnits)
                .setTotalWorkUnits(task.getTotalWorkUnits())
                .setPercentage(
                        completedWorkUnits * 100.0
                                / task.getTotalWorkUnits()
                )
                .setState(TaskState.TASK_STATE_RUNNING)
                .setPartialValue(partialValue)
                .setFinalUpdate(false)
                .setSuccessful(true)
                .setCoordinatorTerm(
                        task.getCoordinatorTerm()
                )
                .setAttempt(task.getAttempt())
                .build();
    }

    private TaskProgress finalProgress(
            TaskRequest task,
            long partialValue
    ) {
        return TaskProgress.newBuilder()
                .setJobId(task.getJobId())
                .setTaskId(task.getTaskId())
                .setWorkerId(task.getAssignedWorkerId())
                .setCompletedWorkUnits(
                        task.getTotalWorkUnits()
                )
                .setTotalWorkUnits(
                        task.getTotalWorkUnits()
                )
                .setPercentage(100.0)
                .setState(TaskState.TASK_STATE_COMPLETED)
                .setPartialValue(partialValue)
                .setFinalUpdate(true)
                .setSuccessful(true)
                .setMessage("Task completed")
                .setCoordinatorTerm(
                        task.getCoordinatorTerm()
                )
                .setAttempt(task.getAttempt())
                .build();
    }
}