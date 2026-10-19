package fj.ac.usp.cs324.distrilab.client;

import fj.ac.usp.cs324.distrilab.bootstrap.grpc.GrpcBootstrapClient;
import fj.ac.usp.cs324.distrilab.common.model.JobRequest;
import fj.ac.usp.cs324.distrilab.common.model.JobResult;
import fj.ac.usp.cs324.distrilab.common.model.JobType;
import fj.ac.usp.cs324.distrilab.grpc.JobIdRequest;
import fj.ac.usp.cs324.distrilab.grpc.JobProgress;
import fj.ac.usp.cs324.distrilab.grpc.JobServiceGrpc;
import fj.ac.usp.cs324.distrilab.grpc.JobSnapshot;
import fj.ac.usp.cs324.distrilab.grpc.JobSubmissionReply;
import fj.ac.usp.cs324.distrilab.grpc.WorkerInfo;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.rmi.RemoteException;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public final class ClientConnection {

    private static final Duration SUBMISSION_DEADLINE =
            Duration.ofSeconds(5);

    private static final Duration PROGRESS_DEADLINE =
            Duration.ofMinutes(10);

    private final String bootstrapHost;
    private final int bootstrapPort;

    public ClientConnection(
            String bootstrapHost,
            int bootstrapPort
    ) {
        if (bootstrapHost == null
                || bootstrapHost.isBlank()) {
            throw new IllegalArgumentException(
                    "Bootstrap host cannot be blank"
            );
        }

        if (bootstrapPort < 1 || bootstrapPort > 65_535) {
            throw new IllegalArgumentException(
                    "Bootstrap port must be between 1 and 65535"
            );
        }

        this.bootstrapHost = bootstrapHost.trim();
        this.bootstrapPort = bootstrapPort;
    }

    public JobResult submitJob(JobRequest request)
            throws RemoteException {

        return submitJob(request, progress -> {
        });
    }

    public JobResult submitJob(
            JobRequest request,
            Consumer<JobProgress> progressConsumer
    ) throws RemoteException {

        Objects.requireNonNull(
                request,
                "Job request cannot be null"
        );

        Objects.requireNonNull(
                progressConsumer,
                "progressConsumer cannot be null"
        );

        try (GrpcBootstrapClient bootstrapClient =
                     new GrpcBootstrapClient(
                             bootstrapHost,
                             bootstrapPort
                     )) {

            List<WorkerInfo> workers =
                    bootstrapClient.listActiveWorkers();

            if (workers.isEmpty()) {
                throw new RemoteException(
                        "No active workers are available"
                );
            }

            StatusRuntimeException lastFailure = null;

            for (WorkerInfo worker : workers) {
                try {
                    return submitThroughWorker(
                            worker,
                            request,
                            progressConsumer
                    );
                } catch (StatusRuntimeException exception) {
                    lastFailure = exception;
                }
            }

            throw new RemoteException(
                    "Unable to submit the job through any active worker",
                    lastFailure
            );
        } catch (StatusRuntimeException exception) {
            throw new RemoteException(
                    "Unable to contact the gRPC Bootstrap Node",
                    exception
            );
        }
    }

    private JobResult submitThroughWorker(
            WorkerInfo worker,
            JobRequest request,
            Consumer<JobProgress> progressConsumer
    ) throws RemoteException {

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(
                        worker.getHost(),
                        worker.getGrpcPort()
                )
                .usePlaintext()
                .build();

        try {
            JobServiceGrpc.JobServiceBlockingStub stub =
                    JobServiceGrpc.newBlockingStub(channel);

            fj.ac.usp.cs324.distrilab.grpc.JobRequest grpcRequest =
                    toGrpcRequest(request);

            JobSubmissionReply submissionReply = stub
                    .withDeadlineAfter(
                            SUBMISSION_DEADLINE.toMillis(),
                            TimeUnit.MILLISECONDS
                    )
                    .submitJob(grpcRequest);

            if (!submissionReply.getAccepted()) {
                throw new RemoteException(
                        submissionReply.getMessage().isBlank()
                                ? "The job was rejected"
                                : submissionReply.getMessage()
                );
            }

            JobIdRequest progressRequest =
                    JobIdRequest.newBuilder()
                            .setJobId(request.jobId())
                            .build();

            Iterator<JobProgress> progressUpdates = stub
                    .withDeadlineAfter(
                            PROGRESS_DEADLINE.toMillis(),
                            TimeUnit.MILLISECONDS
                    )
                    .streamJobProgress(progressRequest);

            while (progressUpdates.hasNext()) {
                JobProgress progress =
                        progressUpdates.next();

                progressConsumer.accept(progress);

                if (progress.hasResult()) {
                    return fromGrpcResult(
                            progress.getResult()
                    );
                }
            }

            JobSnapshot snapshot = stub
                    .withDeadlineAfter(
                            SUBMISSION_DEADLINE.toMillis(),
                            TimeUnit.MILLISECONDS
                    )
                    .getJobStatus(progressRequest);

            if (snapshot.hasResult()) {
                return fromGrpcResult(snapshot.getResult());
            }

            throw new RemoteException(
                    "The progress stream ended without a final result"
            );
        } finally {
            shutdownChannel(channel);
        }
    }

    private static
    fj.ac.usp.cs324.distrilab.grpc.JobRequest
    toGrpcRequest(JobRequest request) {

        return fj.ac.usp.cs324.distrilab.grpc.JobRequest
                .newBuilder()
                .setJobId(request.jobId())
                .setJobType(toGrpcJobType(request.jobType()))
                .addAllNumbers(request.numbers())
                .setRangeStart(request.rangeStart())
                .setRangeEnd(request.rangeEnd())
                .build();
    }

    private static
    fj.ac.usp.cs324.distrilab.grpc.JobType
    toGrpcJobType(JobType jobType) {

        return switch (jobType) {
            case MAX ->
                    fj.ac.usp.cs324.distrilab.grpc.JobType
                            .JOB_TYPE_MAX;

            case PRIMESUM ->
                    fj.ac.usp.cs324.distrilab.grpc.JobType
                            .JOB_TYPE_PRIMESUM;

            case PRIMECOUNT ->
                    fj.ac.usp.cs324.distrilab.grpc.JobType
                            .JOB_TYPE_PRIMECOUNT;
        };
    }

    private static JobType fromGrpcJobType(
            fj.ac.usp.cs324.distrilab.grpc.JobType jobType
    ) throws RemoteException {

        return switch (jobType) {
            case JOB_TYPE_MAX -> JobType.MAX;
            case JOB_TYPE_PRIMESUM -> JobType.PRIMESUM;
            case JOB_TYPE_PRIMECOUNT -> JobType.PRIMECOUNT;

            default -> throw new RemoteException(
                    "Received an unknown job type"
            );
        };
    }

    private static JobResult fromGrpcResult(
            fj.ac.usp.cs324.distrilab.grpc.JobResult result
    ) throws RemoteException {

        return new JobResult(
                result.getJobId(),
                fromGrpcJobType(result.getJobType()),
                result.getValue(),
                result.getSuccessful(),
                result.getMessage()
        );
    }

    private static void shutdownChannel(
            ManagedChannel channel
    ) {
        channel.shutdown();

        try {
            if (!channel.awaitTermination(
                    2,
                    TimeUnit.SECONDS
            )) {
                channel.shutdownNow();

                channel.awaitTermination(
                        2,
                        TimeUnit.SECONDS
                );
            }
        } catch (InterruptedException exception) {
            channel.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}