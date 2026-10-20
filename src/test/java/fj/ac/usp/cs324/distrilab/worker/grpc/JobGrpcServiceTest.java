package fj.ac.usp.cs324.distrilab.worker.grpc;

import fj.ac.usp.cs324.distrilab.grpc.JobType;
import fj.ac.usp.cs324.distrilab.grpc.TaskProgress;
import fj.ac.usp.cs324.distrilab.grpc.TaskRequest;
import fj.ac.usp.cs324.distrilab.grpc.TaskState;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class JobGrpcServiceTest {

    private final JobGrpcService service =
            new JobGrpcService(42);

    @Test
    void executeTaskStreamsRealProgressAndFinalResult() {
        TaskRequest request = TaskRequest.newBuilder()
                .setJobId("job-1")
                .setTaskId("task-1")
                .setJobType(JobType.JOB_TYPE_MAX)
                .addAllNumbers(List.of(5L, 12L, 3L))
                .setAssignedWorkerId(42)
                .setAttempt(2)
                .setCoordinatorTerm(7L)
                .setTotalWorkUnits(3L)
                .build();

        RecordingObserver observer = new RecordingObserver();

        service.executeTask(request, observer);

        assertNull(observer.error);
        assertTrue(observer.completed);
        assertTrue(observer.updates.size() >= 2);

        TaskProgress first = observer.updates.getFirst();

        assertEquals(0L, first.getCompletedWorkUnits());
        assertEquals(3L, first.getTotalWorkUnits());
        assertEquals(TaskState.TASK_STATE_RUNNING, first.getState());
        assertFalse(first.getFinalUpdate());

        assertTrue(observer.updates.stream().anyMatch(update ->
                update.getCompletedWorkUnits() > 0L
                        && !update.getFinalUpdate()
        ));

        TaskProgress last = observer.updates.getLast();

        assertEquals("job-1", last.getJobId());
        assertEquals("task-1", last.getTaskId());
        assertEquals(42, last.getWorkerId());
        assertEquals(3L, last.getCompletedWorkUnits());
        assertEquals(3L, last.getTotalWorkUnits());
        assertEquals(100.0, last.getPercentage(), 0.001);
        assertEquals(12L, last.getPartialValue());
        assertEquals(TaskState.TASK_STATE_COMPLETED, last.getState());
        assertTrue(last.getFinalUpdate());
        assertTrue(last.getSuccessful());
        assertEquals(2, last.getAttempt());
        assertEquals(7L, last.getCoordinatorTerm());
    }

    @Test
    void failedCalculationSendsFinalFailureUpdate() {
        TaskRequest request = TaskRequest.newBuilder()
                .setJobId("job-2")
                .setTaskId("task-2")
                .setJobType(JobType.JOB_TYPE_PRIMECOUNT)
                .setAssignedWorkerId(42)
                .setAttempt(1)
                .setCoordinatorTerm(8L)
                .setTotalWorkUnits(0L)
                .build();

        RecordingObserver observer = new RecordingObserver();

        service.executeTask(request, observer);

        assertNull(observer.error);
        assertTrue(observer.completed);
        assertEquals(1, observer.updates.size());

        TaskProgress last = observer.updates.getFirst();

        assertEquals(TaskState.TASK_STATE_FAILED, last.getState());
        assertTrue(last.getFinalUpdate());
        assertFalse(last.getSuccessful());
        assertTrue(last.getMessage().contains("cannot be empty"));
    }

    @Test
    void invalidAttemptReturnsInvalidArgumentStatus() {
        TaskRequest request = TaskRequest.newBuilder()
                .setJobId("job-3")
                .setTaskId("task-3")
                .setJobType(JobType.JOB_TYPE_MAX)
                .addNumbers(10L)
                .setAttempt(0)
                .build();

        RecordingObserver observer = new RecordingObserver();

        service.executeTask(request, observer);

        assertFalse(observer.completed);
        assertTrue(observer.updates.isEmpty());
        assertNotNull(observer.error);

        assertEquals(
                Status.Code.INVALID_ARGUMENT,
                Status.fromThrowable(observer.error).getCode()
        );
    }

    private static final class RecordingObserver
            implements StreamObserver<TaskProgress> {

        private final List<TaskProgress> updates =
                new ArrayList<>();

        private Throwable error;
        private boolean completed;

        @Override
        public void onNext(TaskProgress value) {
            updates.add(value);
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
