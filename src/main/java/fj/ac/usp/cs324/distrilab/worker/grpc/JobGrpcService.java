package fj.ac.usp.cs324.distrilab.worker.grpc;

import fj.ac.usp.cs324.distrilab.common.model.JobRequest;
import fj.ac.usp.cs324.distrilab.common.model.JobResult;
import fj.ac.usp.cs324.distrilab.grpc.JobIdRequest;
import fj.ac.usp.cs324.distrilab.grpc.JobProgress;
import fj.ac.usp.cs324.distrilab.grpc.JobServiceGrpc;
import fj.ac.usp.cs324.distrilab.grpc.JobSnapshot;
import fj.ac.usp.cs324.distrilab.grpc.JobState;
import fj.ac.usp.cs324.distrilab.grpc.TaskProgress;
import fj.ac.usp.cs324.distrilab.grpc.TaskRequest;
import fj.ac.usp.cs324.distrilab.grpc.TaskState;
import fj.ac.usp.cs324.distrilab.worker.job.JobEngine;
import fj.ac.usp.cs324.distrilab.worker.job.JobProgressTracker;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public final class JobGrpcService
        extends JobServiceGrpc.JobServiceImplBase {

    private final int workerId;
    private final JobEngine jobEngine;
    private final JobProgressTracker progressTracker;

    public JobGrpcService(int workerId) {
        this(
                workerId,
                new JobEngine(),
                new JobProgressTracker()
        );
    }

    public JobGrpcService(
            int workerId,
            JobProgressTracker progressTracker
    ) {
        this(
                workerId,
                new JobEngine(),
                progressTracker
        );
    }

    JobGrpcService(
            int workerId,
            JobEngine jobEngine
    ) {
        this(
                workerId,
                jobEngine,
                new JobProgressTracker()
        );
    }

    JobGrpcService(
            int workerId,
            JobEngine jobEngine,
            JobProgressTracker progressTracker
    ) {
        this.workerId = workerId;

        this.jobEngine = Objects.requireNonNull(
                jobEngine,
                "Job engine cannot be null"
        );

        this.progressTracker = Objects.requireNonNull(
                progressTracker,
                "Progress tracker cannot be null"
        );
    }

    @Override
    public void executeTask(
            TaskRequest request,
            StreamObserver<TaskProgress> responseObserver
    ) {
        Objects.requireNonNull(
                responseObserver,
                "Response observer cannot be null"
        );

        try {
            validateTaskRequest(request);

            JobRequest localRequest = toLocalRequest(request);

            AtomicReference<JobEngine.CalculationProgress>
                    latestProgress = new AtomicReference<>();

            JobResult result = jobEngine.executeJob(
                    localRequest,
                    progress -> {
                        latestProgress.set(progress);

                        if (isCancelled(responseObserver)) {
                            throw new CancellationException(
                                    "Client cancelled task stream"
                            );
                        }

                        if (progress.completedWorkUnits()
                                < progress.totalWorkUnits()) {

                            TaskProgress update = createProgress(
                                    request,
                                    progress,
                                    TaskState.TASK_STATE_RUNNING,
                                    false,
                                    false,
                                    "Task is running"
                            );

                            recordAndSendProgress(
                                    update,
                                    responseObserver
                            );
                        }
                    }
            );

            if (isCancelled(responseObserver)) {
                return;
            }

            JobEngine.CalculationProgress finalProgress =
                    latestProgress.get();

            if (finalProgress == null) {
                finalProgress =
                        new JobEngine.CalculationProgress(
                                0L,
                                determineTotalWorkUnits(request),
                                0L
                        );
            }

            long finalValue = result.successful()
                    ? result.value()
                    : finalProgress.partialValue();

            JobEngine.CalculationProgress completedProgress =
                    new JobEngine.CalculationProgress(
                            finalProgress.completedWorkUnits(),
                            finalProgress.totalWorkUnits(),
                            finalValue
                    );

            TaskState finalState = result.successful()
                    ? TaskState.TASK_STATE_COMPLETED
                    : TaskState.TASK_STATE_FAILED;

            TaskProgress finalUpdate = createProgress(
                    request,
                    completedProgress,
                    finalState,
                    true,
                    result.successful(),
                    result.message()
            );

            recordAndSendProgress(
                    finalUpdate,
                    responseObserver
            );

            responseObserver.onCompleted();
        } catch (CancellationException exception) {
            if (!isCancelled(responseObserver)) {
                responseObserver.onError(
                        Status.CANCELLED
                                .withDescription(
                                        exception.getMessage()
                                )
                                .asRuntimeException()
                );
            }
        } catch (IllegalArgumentException exception) {
            responseObserver.onError(
                    Status.INVALID_ARGUMENT
                            .withDescription(
                                    exception.getMessage()
                            )
                            .asRuntimeException()
            );
        } catch (RuntimeException exception) {
            responseObserver.onError(
                    Status.INTERNAL
                            .withDescription(
                                    "Task execution failed"
                            )
                            .withCause(exception)
                            .asRuntimeException()
            );
        }
    }

    @Override
    public void streamJobProgress(
            JobIdRequest request,
            StreamObserver<JobProgress> responseObserver
    ) {
        Objects.requireNonNull(
                responseObserver,
                "Response observer cannot be null"
        );

        try {
            validateJobIdRequest(request);

            AtomicBoolean streamClosed =
                    new AtomicBoolean(false);

            AtomicReference<AutoCloseable> subscription =
                    new AtomicReference<>();

            Object responseLock = new Object();

            if (responseObserver
                    instanceof ServerCallStreamObserver<?>
                    serverObserver) {

                serverObserver.setOnCancelHandler(() -> {
                    streamClosed.set(true);
                    closeSubscription(subscription.get());
                });
            }

            AutoCloseable newSubscription =
                    progressTracker.subscribe(
                            request.getJobId(),
                            progress -> {
                                synchronized (responseLock) {
                                    if (streamClosed.get()
                                            || isCancelled(
                                            responseObserver
                                    )) {
                                        streamClosed.set(true);

                                        closeSubscription(
                                                subscription.get()
                                        );

                                        return;
                                    }

                                    responseObserver.onNext(progress);

                                    if (isFinalState(
                                            progress.getState()
                                    )) {
                                        streamClosed.set(true);
                                        responseObserver.onCompleted();

                                        closeSubscription(
                                                subscription.get()
                                        );
                                    }
                                }
                            }
                    );

            subscription.set(newSubscription);

            if (streamClosed.get()) {
                closeSubscription(newSubscription);
            }
        } catch (IllegalArgumentException exception) {
            responseObserver.onError(
                    Status.NOT_FOUND
                            .withDescription(
                                    exception.getMessage()
                            )
                            .asRuntimeException()
            );
        } catch (RuntimeException exception) {
            responseObserver.onError(
                    Status.INTERNAL
                            .withDescription(
                                    "Unable to stream job progress"
                            )
                            .withCause(exception)
                            .asRuntimeException()
            );
        }
    }

    @Override
    public void getJobStatus(
            JobIdRequest request,
            StreamObserver<JobSnapshot> responseObserver
    ) {
        Objects.requireNonNull(
                responseObserver,
                "Response observer cannot be null"
        );

        try {
            validateJobIdRequest(request);

            JobSnapshot snapshot =
                    progressTracker.getSnapshot(
                            request.getJobId()
                    );

            responseObserver.onNext(snapshot);
            responseObserver.onCompleted();
        } catch (IllegalArgumentException exception) {
            responseObserver.onError(
                    Status.NOT_FOUND
                            .withDescription(
                                    exception.getMessage()
                            )
                            .asRuntimeException()
            );
        } catch (RuntimeException exception) {
            responseObserver.onError(
                    Status.INTERNAL
                            .withDescription(
                                    "Unable to get job status"
                            )
                            .withCause(exception)
                            .asRuntimeException()
            );
        }
    }

    public JobProgressTracker getProgressTracker() {
        return progressTracker;
    }

    private void recordAndSendProgress(
            TaskProgress progress,
            StreamObserver<TaskProgress> responseObserver
    ) {
        if (progressTracker.containsJob(
                progress.getJobId()
        )) {
            progressTracker.recordTaskProgress(progress);
        }

        responseObserver.onNext(progress);
    }

    private void validateTaskRequest(TaskRequest request) {
        if (request == null) {
            throw new IllegalArgumentException(
                    "Task request cannot be null"
            );
        }

        if (request.getJobId().isBlank()) {
            throw new IllegalArgumentException(
                    "Job ID cannot be blank"
            );
        }

        if (request.getTaskId().isBlank()) {
            throw new IllegalArgumentException(
                    "Task ID cannot be blank"
            );
        }

        if (request.getAttempt() < 1) {
            throw new IllegalArgumentException(
                    "Task attempt must be at least 1"
            );
        }
    }

    private void validateJobIdRequest(
            JobIdRequest request
    ) {
        if (request == null
                || request.getJobId().isBlank()) {
            throw new IllegalArgumentException(
                    "Job ID cannot be blank"
            );
        }
    }

    private JobRequest toLocalRequest(
            TaskRequest request
    ) {
        fj.ac.usp.cs324.distrilab.common.model.JobType
                jobType = switch (request.getJobType()) {

            case JOB_TYPE_MAX ->
                    fj.ac.usp.cs324.distrilab.common.model
                            .JobType.MAX;

            case JOB_TYPE_PRIMESUM ->
                    fj.ac.usp.cs324.distrilab.common.model
                            .JobType.PRIMESUM;

            case JOB_TYPE_PRIMECOUNT ->
                    fj.ac.usp.cs324.distrilab.common.model
                            .JobType.PRIMECOUNT;

            default -> throw new IllegalArgumentException(
                    "A valid job type is required"
            );
        };

        return new JobRequest(
                request.getJobId(),
                jobType,
                request.getNumbersList(),
                request.getRangeStart(),
                request.getRangeEnd()
        );
    }

    private TaskProgress createProgress(
            TaskRequest request,
            JobEngine.CalculationProgress progress,
            TaskState state,
            boolean finalUpdate,
            boolean successful,
            String message
    ) {
        double percentage =
                progress.totalWorkUnits() <= 0L
                        ? 0.0
                        : Math.min(
                        100.0,
                        progress.completedWorkUnits()
                                * 100.0
                                / progress.totalWorkUnits()
                );

        return TaskProgress.newBuilder()
                .setJobId(request.getJobId())
                .setTaskId(request.getTaskId())
                .setWorkerId(workerId)
                .setCompletedWorkUnits(
                        progress.completedWorkUnits()
                )
                .setTotalWorkUnits(
                        progress.totalWorkUnits()
                )
                .setPercentage(percentage)
                .setState(state)
                .setPartialValue(
                        progress.partialValue()
                )
                .setFinalUpdate(finalUpdate)
                .setSuccessful(successful)
                .setMessage(
                        message == null ? "" : message
                )
                .setCoordinatorTerm(
                        request.getCoordinatorTerm()
                )
                .setAttempt(request.getAttempt())
                .build();
    }

    private long determineTotalWorkUnits(
            TaskRequest request
    ) {
        return switch (request.getJobType()) {
            case JOB_TYPE_MAX, JOB_TYPE_PRIMECOUNT ->
                    request.getNumbersCount();

            case JOB_TYPE_PRIMESUM -> {
                try {
                    yield Math.addExact(
                            Math.subtractExact(
                                    request.getRangeEnd(),
                                    request.getRangeStart()
                            ),
                            1L
                    );
                } catch (ArithmeticException exception) {
                    yield 0L;
                }
            }

            default -> 0L;
        };
    }

    private boolean isFinalState(JobState state) {
        return state == JobState.JOB_STATE_COMPLETED
                || state == JobState.JOB_STATE_FAILED;
    }

    private boolean isCancelled(
            StreamObserver<?> responseObserver
    ) {
        return responseObserver
                instanceof ServerCallStreamObserver<?>
                serverObserver
                && serverObserver.isCancelled();
    }

    private void closeSubscription(
            AutoCloseable subscription
    ) {
        if (subscription == null) {
            return;
        }

        try {
            subscription.close();
        } catch (Exception ignored) {
            // Closing a listener should not fail the RPC.
        }
    }
}