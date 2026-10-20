package fj.ac.usp.cs324.distrilab.worker.grpc;

import fj.ac.usp.cs324.distrilab.grpc.JobIdRequest;
import fj.ac.usp.cs324.distrilab.grpc.JobProgress;
import fj.ac.usp.cs324.distrilab.grpc.JobRequest;
import fj.ac.usp.cs324.distrilab.grpc.JobSnapshot;
import fj.ac.usp.cs324.distrilab.grpc.JobState;
import fj.ac.usp.cs324.distrilab.grpc.JobType;
import fj.ac.usp.cs324.distrilab.grpc.TaskProgress;
import fj.ac.usp.cs324.distrilab.grpc.TaskRequest;
import fj.ac.usp.cs324.distrilab.grpc.TaskState;
import fj.ac.usp.cs324.distrilab.worker.job.JobProgressTracker;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobGrpcProgressServiceTest {

    @Test
    void streamsJobProgressUntilJobCompletes() {
        JobProgressTracker tracker =
                new JobProgressTracker();

        JobGrpcService service =
                new JobGrpcService(1, tracker);

        JobRequest job = createJob("stream-job");

        TaskRequest task = createTask(
                "stream-job",
                "task-1"
        );

        tracker.registerJob(
                job,
                List.of(task),
                1,
                1L
        );

        RecordingObserver<JobProgress> observer =
                new RecordingObserver<>();

        service.streamJobProgress(
                JobIdRequest.newBuilder()
                        .setJobId("stream-job")
                        .build(),
                observer
        );

        tracker.recordTaskProgress(
                createProgress(
                        task,
                        5L,
                        5L,
                        false
                )
        );

        tracker.recordTaskProgress(
                createProgress(
                        task,
                        10L,
                        17L,
                        true
                )
        );

        assertNull(observer.error);
        assertTrue(observer.completed);
        assertEquals(3, observer.values.size());

        assertEquals(
                JobState.JOB_STATE_ACCEPTED,
                observer.values.getFirst().getState()
        );

        assertEquals(
                50.0,
                observer.values.get(1)
                        .getOverallPercentage()
        );

        JobProgress finalProgress =
                observer.values.getLast();

        assertEquals(
                JobState.JOB_STATE_COMPLETED,
                finalProgress.getState()
        );

        assertTrue(finalProgress.hasResult());
        assertEquals(
                17L,
                finalProgress.getResult().getValue()
        );
    }

    @Test
    void getJobStatusReturnsCurrentSnapshot() {
        JobProgressTracker tracker =
                new JobProgressTracker();

        JobGrpcService service =
                new JobGrpcService(1, tracker);

        JobRequest job = createJob("status-job");

        TaskRequest task = createTask(
                "status-job",
                "task-1"
        );

        tracker.registerJob(
                job,
                List.of(task),
                1,
                1L
        );

        tracker.recordTaskProgress(
                createProgress(
                        task,
                        4L,
                        5L,
                        false
                )
        );

        RecordingObserver<JobSnapshot> observer =
                new RecordingObserver<>();

        service.getJobStatus(
                JobIdRequest.newBuilder()
                        .setJobId("status-job")
                        .build(),
                observer
        );

        assertNull(observer.error);
        assertTrue(observer.completed);
        assertEquals(1, observer.values.size());

        JobSnapshot snapshot =
                observer.values.getFirst();

        assertEquals(
                JobState.JOB_STATE_RUNNING,
                snapshot.getState()
        );

        assertEquals(4L, snapshot.getCompletedWorkUnits());
        assertEquals(10L, snapshot.getTotalWorkUnits());
        assertEquals(1, snapshot.getTasksCount());
        assertEquals(
                "task-1",
                snapshot.getTasks(0)
                        .getRequest()
                        .getTaskId()
        );
    }

    @Test
    void executeTaskUpdatesRegisteredJobProgress() {
        JobProgressTracker tracker =
                new JobProgressTracker();

        JobGrpcService service =
                new JobGrpcService(1, tracker);

        JobRequest job = createJob("execute-job");

        TaskRequest task = createTask(
                "execute-job",
                "task-1"
        );

        tracker.registerJob(
                job,
                List.of(task),
                1,
                1L
        );

        RecordingObserver<TaskProgress> taskObserver =
                new RecordingObserver<>();

        service.executeTask(task, taskObserver);

        assertNull(taskObserver.error);
        assertTrue(taskObserver.completed);

        JobProgress progress =
                tracker.getProgress("execute-job");

        assertEquals(
                JobState.JOB_STATE_COMPLETED,
                progress.getState()
        );

        assertTrue(progress.hasResult());
        assertEquals(
                17L,
                progress.getResult().getValue()
        );
    }

    @Test
    void unknownJobStatusReturnsNotFound() {
        JobProgressTracker tracker =
                new JobProgressTracker();

        JobGrpcService service =
                new JobGrpcService(1, tracker);

        RecordingObserver<JobSnapshot> observer =
                new RecordingObserver<>();

        service.getJobStatus(
                JobIdRequest.newBuilder()
                        .setJobId("missing-job")
                        .build(),
                observer
        );

        StatusRuntimeException exception =
                assertInstanceOf(
                        StatusRuntimeException.class,
                        observer.error
                );

        assertEquals(
                Status.Code.NOT_FOUND,
                exception.getStatus().getCode()
        );
    }

    private JobRequest createJob(String jobId) {
        return JobRequest.newBuilder()
                .setJobId(jobId)
                .setJobType(JobType.JOB_TYPE_PRIMESUM)
                .setRangeStart(1L)
                .setRangeEnd(10L)
                .build();
    }

    private TaskRequest createTask(
            String jobId,
            String taskId
    ) {
        return TaskRequest.newBuilder()
                .setJobId(jobId)
                .setTaskId(taskId)
                .setJobType(JobType.JOB_TYPE_PRIMESUM)
                .setRangeStart(1L)
                .setRangeEnd(10L)
                .setAssignedWorkerId(1)
                .setAttempt(1)
                .setCoordinatorTerm(1L)
                .setTotalWorkUnits(10L)
                .build();
    }

    private TaskProgress createProgress(
            TaskRequest task,
            long completed,
            long partialValue,
            boolean finalUpdate
    ) {
        return TaskProgress.newBuilder()
                .setJobId(task.getJobId())
                .setTaskId(task.getTaskId())
                .setWorkerId(
                        task.getAssignedWorkerId()
                )
                .setCompletedWorkUnits(completed)
                .setTotalWorkUnits(
                        task.getTotalWorkUnits()
                )
                .setPercentage(
                        completed * 100.0
                                / task.getTotalWorkUnits()
                )
                .setState(
                        finalUpdate
                                ? TaskState.TASK_STATE_COMPLETED
                                : TaskState.TASK_STATE_RUNNING
                )
                .setPartialValue(partialValue)
                .setFinalUpdate(finalUpdate)
                .setSuccessful(true)
                .setMessage(
                        finalUpdate
                                ? "Task completed"
                                : "Task is running"
                )
                .setCoordinatorTerm(
                        task.getCoordinatorTerm()
                )
                .setAttempt(task.getAttempt())
                .build();
    }

    private static final class RecordingObserver<T>
            implements StreamObserver<T> {

        private final List<T> values =
                new ArrayList<>();

        private Throwable error;
        private boolean completed;

        @Override
        public void onNext(T value) {
            values.add(value);
        }

        @Override
        public void onError(Throwable throwable) {
            error = throwable;
        }

        @Override
        public void onCompleted() {
            completed = true;
        }
    }
}
