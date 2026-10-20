package fj.ac.usp.cs324.distrilab.worker.job;

import fj.ac.usp.cs324.distrilab.grpc.JobProgress;
import fj.ac.usp.cs324.distrilab.grpc.JobRequest;
import fj.ac.usp.cs324.distrilab.grpc.JobResult;
import fj.ac.usp.cs324.distrilab.grpc.JobSnapshot;
import fj.ac.usp.cs324.distrilab.grpc.JobState;
import fj.ac.usp.cs324.distrilab.grpc.JobType;
import fj.ac.usp.cs324.distrilab.grpc.TaskProgress;
import fj.ac.usp.cs324.distrilab.grpc.TaskRequest;
import fj.ac.usp.cs324.distrilab.grpc.TaskSnapshot;
import fj.ac.usp.cs324.distrilab.grpc.TaskState;
import fj.ac.usp.cs324.distrilab.grpc.WorkerProgress;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public class JobProgressTracker {

    private final ConcurrentHashMap<String, JobRecord> jobs =
            new ConcurrentHashMap<>();

    public boolean registerJob(
            JobRequest request,
            List<TaskRequest> tasks,
            int coordinatorId,
            long coordinatorTerm
    ) {
        Objects.requireNonNull(request, "Job request cannot be null");
        Objects.requireNonNull(tasks, "Task list cannot be null");

        if (request.getJobId().isBlank()) {
            throw new IllegalArgumentException("Job ID cannot be blank");
        }

        if (tasks.isEmpty()) {
            throw new IllegalArgumentException(
                    "A job must contain at least one task"
            );
        }

        JobRecord newRecord = new JobRecord(
                request,
                tasks,
                coordinatorId,
                coordinatorTerm
        );

        return jobs.putIfAbsent(request.getJobId(), newRecord) == null;
    }

    public boolean containsJob(String jobId) {
        return jobs.containsKey(jobId);
    }

    public boolean recordTaskProgress(TaskProgress update) {
        Objects.requireNonNull(update, "Task progress cannot be null");

        JobRecord record = requireJob(update.getJobId());
        JobProgress progress;

        synchronized (record) {
            TaskRecord task = record.tasks.get(update.getTaskId());

            if (task == null) {
                throw new IllegalArgumentException(
                        "Unknown task ID: " + update.getTaskId()
                );
            }

            if (update.getAttempt() != task.request.getAttempt()) {
                return false;
            }

            if (update.getCoordinatorTerm()
                    != record.coordinatorTerm) {
                return false;
            }

            if (task.resultRecorded) {
                return false;
            }

            int assignedWorker = task.request.getAssignedWorkerId();

            if (assignedWorker != 0
                    && update.getWorkerId() != assignedWorker) {
                return false;
            }

            long reportedTotal = Math.max(
                    0L,
                    update.getTotalWorkUnits()
            );

            if (task.totalWorkUnits == 0L) {
                task.totalWorkUnits = reportedTotal;
            }

            long maximumCompleted = task.totalWorkUnits == 0L
                    ? Math.max(0L, update.getCompletedWorkUnits())
                    : task.totalWorkUnits;

            task.completedWorkUnits = Math.max(
                    task.completedWorkUnits,
                    Math.min(
                            Math.max(0L, update.getCompletedWorkUnits()),
                            maximumCompleted
                    )
            );

            task.partialValue = update.getPartialValue();
            task.message = update.getMessage();

            if (update.getState()
                    != TaskState.TASK_STATE_UNSPECIFIED) {
                task.state = update.getState();
            } else {
                task.state = TaskState.TASK_STATE_RUNNING;
            }

            if (update.getFinalUpdate()) {
                if (update.getSuccessful()) {
                    task.state = TaskState.TASK_STATE_COMPLETED;
                    task.completedWorkUnits = task.totalWorkUnits;
                    task.resultRecorded = true;
                } else {
                    task.state = TaskState.TASK_STATE_FAILED;
                }
            }

            record.recalculate();
            progress = record.createProgress();
        }

        record.publish(progress);
        return true;
    }

    public void reassignTask(
            String jobId,
            String taskId,
            int newWorkerId,
            int newAttempt,
            int coordinatorId,
            long coordinatorTerm
    ) {
        JobRecord record = requireJob(jobId);
        JobProgress progress;

        synchronized (record) {
            TaskRecord task = record.tasks.get(taskId);

            if (task == null) {
                throw new IllegalArgumentException(
                        "Unknown task ID: " + taskId
                );
            }

            if (newAttempt <= task.request.getAttempt()) {
                throw new IllegalArgumentException(
                        "The new attempt must be greater than the old attempt"
                );
            }

            if (coordinatorTerm < record.coordinatorTerm) {
                throw new IllegalArgumentException(
                        "Cannot use an older coordinator term"
                );
            }

            record.coordinatorId = coordinatorId;
            record.coordinatorTerm = coordinatorTerm;
            record.state = JobState.JOB_STATE_RECOVERING;
            record.result = null;

            task.request = task.request.toBuilder()
                    .setAssignedWorkerId(newWorkerId)
                    .setAttempt(newAttempt)
                    .setCoordinatorTerm(coordinatorTerm)
                    .build();

            task.state = TaskState.TASK_STATE_REASSIGNED;
            task.completedWorkUnits = 0L;
            task.partialValue = 0L;
            task.resultRecorded = false;
            task.message = "Task reassigned to worker " + newWorkerId;

            progress = record.createProgress();
        }

        record.publish(progress);
    }

    public JobProgress getProgress(String jobId) {
        JobRecord record = requireJob(jobId);

        synchronized (record) {
            return record.createProgress();
        }
    }

    public JobSnapshot getSnapshot(String jobId) {
        JobRecord record = requireJob(jobId);

        synchronized (record) {
            return record.createSnapshot();
        }
    }

    public AutoCloseable subscribe(
            String jobId,
            Consumer<JobProgress> listener
    ) {
        Objects.requireNonNull(listener, "Listener cannot be null");

        JobRecord record = requireJob(jobId);
        record.listeners.add(listener);

        listener.accept(getProgress(jobId));

        return () -> record.listeners.remove(listener);
    }

    private JobRecord requireJob(String jobId) {
        if (jobId == null || jobId.isBlank()) {
            throw new IllegalArgumentException(
                    "Job ID cannot be blank"
            );
        }

        JobRecord record = jobs.get(jobId);

        if (record == null) {
            throw new IllegalArgumentException(
                    "Unknown job ID: " + jobId
            );
        }

        return record;
    }

    private static final class JobRecord {

        private final JobRequest request;
        private final Map<String, TaskRecord> tasks;
        private final CopyOnWriteArrayList<Consumer<JobProgress>> listeners;

        private int coordinatorId;
        private long coordinatorTerm;
        private JobState state;
        private JobResult result;

        private JobRecord(
                JobRequest request,
                List<TaskRequest> taskRequests,
                int coordinatorId,
                long coordinatorTerm
        ) {
            this.request = request;
            this.coordinatorId = coordinatorId;
            this.coordinatorTerm = coordinatorTerm;
            this.state = JobState.JOB_STATE_ACCEPTED;
            this.tasks = new LinkedHashMap<>();
            this.listeners = new CopyOnWriteArrayList<>();

            for (TaskRequest taskRequest : taskRequests) {
                if (!request.getJobId().equals(
                        taskRequest.getJobId())) {
                    throw new IllegalArgumentException(
                            "Every task must use the job's ID"
                    );
                }

                if (taskRequest.getTaskId().isBlank()) {
                    throw new IllegalArgumentException(
                            "Task ID cannot be blank"
                    );
                }

                if (tasks.put(
                        taskRequest.getTaskId(),
                        new TaskRecord(taskRequest)
                ) != null) {
                    throw new IllegalArgumentException(
                            "Duplicate task ID: "
                                    + taskRequest.getTaskId()
                    );
                }
            }
        }

        private void recalculate() {
            boolean allCompleted = true;
            boolean recovering = false;

            for (TaskRecord task : tasks.values()) {
                if (!task.resultRecorded) {
                    allCompleted = false;
                }

                if (task.state == TaskState.TASK_STATE_FAILED
                        || task.state
                        == TaskState.TASK_STATE_REASSIGNED) {
                    recovering = true;
                }
            }

            if (allCompleted) {
                completeJob();
            } else if (recovering) {
                state = JobState.JOB_STATE_RECOVERING;
            } else {
                state = JobState.JOB_STATE_RUNNING;
            }
        }

        private void completeJob() {
            try {
                long value;

                if (request.getJobType()
                        == JobType.JOB_TYPE_MAX) {
                    value = tasks.values()
                            .stream()
                            .mapToLong(task -> task.partialValue)
                            .max()
                            .orElseThrow();
                } else {
                    value = 0L;

                    for (TaskRecord task : tasks.values()) {
                        value = Math.addExact(
                                value,
                                task.partialValue
                        );
                    }
                }

                result = JobResult.newBuilder()
                        .setJobId(request.getJobId())
                        .setJobType(request.getJobType())
                        .setValue(value)
                        .setSuccessful(true)
                        .setMessage("Job completed successfully")
                        .build();

                state = JobState.JOB_STATE_COMPLETED;
            } catch (ArithmeticException exception) {
                result = JobResult.newBuilder()
                        .setJobId(request.getJobId())
                        .setJobType(request.getJobType())
                        .setValue(0L)
                        .setSuccessful(false)
                        .setMessage("Combined result is too large")
                        .build();

                state = JobState.JOB_STATE_FAILED;
            }
        }

        private JobProgress createProgress() {
            long completed = 0L;
            long total = 0L;

            JobProgress.Builder builder =
                    JobProgress.newBuilder()
                            .setJobId(request.getJobId())
                            .setState(state)
                            .setCoordinatorId(coordinatorId)
                            .setCoordinatorTerm(coordinatorTerm);

            for (TaskRecord task : tasks.values()) {
                completed = safeAdd(
                        completed,
                        task.completedWorkUnits
                );

                total = safeAdd(total, task.totalWorkUnits);

                double percentage = calculatePercentage(
                        task.completedWorkUnits,
                        task.totalWorkUnits
                );

                builder.addWorkerProgress(
                        WorkerProgress.newBuilder()
                                .setWorkerId(
                                        task.request
                                                .getAssignedWorkerId()
                                )
                                .setTaskId(
                                        task.request.getTaskId()
                                )
                                .setCompletedWorkUnits(
                                        task.completedWorkUnits
                                )
                                .setTotalWorkUnits(
                                        task.totalWorkUnits
                                )
                                .setPercentage(percentage)
                                .setState(task.state)
                                .setPartialValue(
                                        task.partialValue
                                )
                                .setMessage(task.message)
                                .build()
                );
            }

            builder.setCompletedWorkUnits(completed)
                    .setTotalWorkUnits(total)
                    .setOverallPercentage(
                            calculatePercentage(completed, total)
                    )
                    .setMessage(progressMessage());

            if (result != null) {
                builder.setResult(result);
            }

            return builder.build();
        }

        private JobSnapshot createSnapshot() {
            long completed = 0L;
            long total = 0L;

            JobSnapshot.Builder builder =
                    JobSnapshot.newBuilder()
                            .setRequest(request)
                            .setState(state);

            for (TaskRecord task : tasks.values()) {
                completed = safeAdd(
                        completed,
                        task.completedWorkUnits
                );

                total = safeAdd(total, task.totalWorkUnits);

                builder.addTasks(
                        TaskSnapshot.newBuilder()
                                .setRequest(task.request)
                                .setState(task.state)
                                .setAssignedWorkerId(
                                        task.request
                                                .getAssignedWorkerId()
                                )
                                .setCompletedWorkUnits(
                                        task.completedWorkUnits
                                )
                                .setTotalWorkUnits(
                                        task.totalWorkUnits
                                )
                                .setPartialValue(
                                        task.partialValue
                                )
                                .setAttempt(
                                        task.request.getAttempt()
                                )
                                .setResultRecorded(
                                        task.resultRecorded
                                )
                                .build()
                );
            }

            builder.setCompletedWorkUnits(completed)
                    .setTotalWorkUnits(total);

            if (result != null) {
                builder.setResult(result);
            }

            return builder.build();
        }

        private String progressMessage() {
            return switch (state) {
                case JOB_STATE_ACCEPTED -> "Job accepted";
                case JOB_STATE_RUNNING -> "Job is running";
                case JOB_STATE_COMPLETED ->
                        "Job completed successfully";
                case JOB_STATE_FAILED -> "Job failed";
                case JOB_STATE_RECOVERING ->
                        "Job is recovering unfinished tasks";
                default -> "";
            };
        }

        private void publish(JobProgress progress) {
            for (Consumer<JobProgress> listener : listeners) {
                try {
                    listener.accept(progress);
                } catch (RuntimeException exception) {
                    listeners.remove(listener);
                }
            }
        }
    }

    private static final class TaskRecord {

        private TaskRequest request;
        private TaskState state;
        private long completedWorkUnits;
        private long totalWorkUnits;
        private long partialValue;
        private boolean resultRecorded;
        private String message;

        private TaskRecord(TaskRequest request) {
            this.request = request;
            this.state = TaskState.TASK_STATE_ASSIGNED;
            this.completedWorkUnits = 0L;
            this.totalWorkUnits = Math.max(
                    0L,
                    request.getTotalWorkUnits()
            );
            this.partialValue = 0L;
            this.resultRecorded = false;
            this.message = "Task assigned";
        }
    }

    private static double calculatePercentage(
            long completed,
            long total
    ) {
        if (total <= 0L) {
            return 0.0;
        }

        return Math.min(
                100.0,
                (completed * 100.0) / total
        );
    }

    private static long safeAdd(long first, long second) {
        try {
            return Math.addExact(first, second);
        } catch (ArithmeticException exception) {
            return Long.MAX_VALUE;
        }
    }
}
