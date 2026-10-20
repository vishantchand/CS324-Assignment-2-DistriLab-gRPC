package fj.ac.usp.cs324.distrilab.client;

import fj.ac.usp.cs324.distrilab.common.model.JobRequest;
import fj.ac.usp.cs324.distrilab.common.model.JobResult;
import fj.ac.usp.cs324.distrilab.common.model.JobType;
import fj.ac.usp.cs324.distrilab.grpc.BootstrapServiceGrpc;
import fj.ac.usp.cs324.distrilab.grpc.Empty;
import fj.ac.usp.cs324.distrilab.grpc.JobIdRequest;
import fj.ac.usp.cs324.distrilab.grpc.JobProgress;
import fj.ac.usp.cs324.distrilab.grpc.JobServiceGrpc;
import fj.ac.usp.cs324.distrilab.grpc.JobState;
import fj.ac.usp.cs324.distrilab.grpc.JobSubmissionReply;
import fj.ac.usp.cs324.distrilab.grpc.WorkerInfo;
import fj.ac.usp.cs324.distrilab.grpc.WorkerList;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClientConnectionTest {

    private Server bootstrapServer;
    private Server workerServer;

    private TestBootstrapService bootstrapService;
    private TestJobService jobService;

    private ClientConnection connection;

    @BeforeEach
    void setUp() throws Exception {
        jobService = new TestJobService();

        workerServer = ServerBuilder
                .forPort(0)
                .addService(jobService)
                .build()
                .start();

        WorkerInfo worker = WorkerInfo.newBuilder()
                .setWorkerId(1)
                .setHost("localhost")
                .setGrpcPort(workerServer.getPort())
                .setWorkerName("Worker-1")
                .build();

        bootstrapService =
                new TestBootstrapService(List.of(worker));

        bootstrapServer = ServerBuilder
                .forPort(0)
                .addService(bootstrapService)
                .build()
                .start();

        connection = new ClientConnection(
                "localhost",
                bootstrapServer.getPort()
        );
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        shutdownServer(bootstrapServer);
        shutdownServer(workerServer);
    }

    @Test
    void submitsJobAndReceivesStreamedProgress() {
        JobRequest request = request();

        List<Double> percentages = new ArrayList<>();
        List<JobState> states = new ArrayList<>();

        JobResult result = connection.submitJob(
                request,
                progress -> {
                    percentages.add(
                            progress.getOverallPercentage()
                    );

                    states.add(progress.getState());
                }
        );

        assertEquals("client-test-job", result.jobId());
        assertEquals(JobType.MAX, result.jobType());
        assertEquals(97L, result.value());
        assertTrue(result.successful());
        assertEquals("Completed successfully", result.message());

        assertEquals(
                List.of(50.0, 100.0),
                percentages
        );

        assertEquals(
                List.of(
                        JobState.JOB_STATE_RUNNING,
                        JobState.JOB_STATE_COMPLETED
                ),
                states
        );
    }

    @Test
    void submitsJobWithoutProgressConsumer() {
        JobResult result = connection.submitJob(request());

        assertEquals("client-test-job", result.jobId());
        assertEquals(JobType.MAX, result.jobType());
        assertEquals(97L, result.value());
        assertTrue(result.successful());
    }

    @Test
    void rejectsMissingSubmissionArguments() {
        assertThrows(
                NullPointerException.class,
                () -> connection.submitJob(null)
        );

        assertThrows(
                NullPointerException.class,
                () -> connection.submitJob(
                        request(),
                        null
                )
        );
    }

    @Test
    void reportsWhenNoWorkersAreAvailable() {
        bootstrapService.setActiveWorkers(List.of());

        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> connection.submitJob(request())
        );

        assertTrue(
                exception.getMessage().contains(
                        "No active workers"
                )
        );
    }

    @Test
    void validatesBootstrapEndpoint() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new ClientConnection("", 5000)
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> new ClientConnection("localhost", 0)
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> new ClientConnection(
                        "localhost",
                        65_536
                )
        );
    }

    private static JobRequest request() {
        return new JobRequest(
                "client-test-job",
                JobType.MAX,
                List.of(14L, 97L, 32L),
                0L,
                0L
        );
    }

    private static void shutdownServer(
            Server server
    ) throws InterruptedException {
        if (server == null) {
            return;
        }

        server.shutdownNow();

        server.awaitTermination(
                2,
                TimeUnit.SECONDS
        );
    }

    private static final class TestBootstrapService
            extends BootstrapServiceGrpc
            .BootstrapServiceImplBase {

        private volatile List<WorkerInfo> activeWorkers;

        private TestBootstrapService(
                List<WorkerInfo> activeWorkers
        ) {
            setActiveWorkers(activeWorkers);
        }

        private void setActiveWorkers(
                List<WorkerInfo> activeWorkers
        ) {
            this.activeWorkers =
                    List.copyOf(activeWorkers);
        }

        @Override
        public void listActiveWorkers(
                Empty request,
                StreamObserver<WorkerList> responseObserver
        ) {
            WorkerList response = WorkerList
                    .newBuilder()
                    .addAllWorkers(activeWorkers)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    private static final class TestJobService
            extends JobServiceGrpc.JobServiceImplBase {

        private volatile
        fj.ac.usp.cs324.distrilab.grpc.JobRequest
                submittedRequest;

        @Override
        public void submitJob(
                fj.ac.usp.cs324.distrilab.grpc.JobRequest request,
                StreamObserver<JobSubmissionReply> responseObserver
        ) {
            submittedRequest = request;

            JobSubmissionReply response =
                    JobSubmissionReply.newBuilder()
                            .setAccepted(true)
                            .setJobId(request.getJobId())
                            .setMessage("Job accepted")
                            .setCoordinatorId(1)
                            .setCoordinatorTerm(1L)
                            .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void streamJobProgress(
                JobIdRequest request,
                StreamObserver<JobProgress> responseObserver
        ) {
            fj.ac.usp.cs324.distrilab.grpc.JobRequest
                    acceptedRequest = submittedRequest;

            if (acceptedRequest == null) {
                responseObserver.onError(
                        new IllegalStateException(
                                "No job was submitted"
                        )
                );
                return;
            }

            JobProgress runningProgress =
                    JobProgress.newBuilder()
                            .setJobId(request.getJobId())
                            .setState(
                                    JobState.JOB_STATE_RUNNING
                            )
                            .setCompletedWorkUnits(1L)
                            .setTotalWorkUnits(2L)
                            .setOverallPercentage(50.0)
                            .setCoordinatorId(1)
                            .setCoordinatorTerm(1L)
                            .setMessage("Job running")
                            .build();

            fj.ac.usp.cs324.distrilab.grpc.JobResult
                    grpcResult =
                    fj.ac.usp.cs324.distrilab.grpc.JobResult
                            .newBuilder()
                            .setJobId(request.getJobId())
                            .setJobType(
                                    acceptedRequest.getJobType()
                            )
                            .setValue(97L)
                            .setSuccessful(true)
                            .setMessage(
                                    "Completed successfully"
                            )
                            .build();

            JobProgress completedProgress =
                    JobProgress.newBuilder()
                            .setJobId(request.getJobId())
                            .setState(
                                    JobState.JOB_STATE_COMPLETED
                            )
                            .setCompletedWorkUnits(2L)
                            .setTotalWorkUnits(2L)
                            .setOverallPercentage(100.0)
                            .setCoordinatorId(1)
                            .setCoordinatorTerm(1L)
                            .setMessage("Job completed")
                            .setResult(grpcResult)
                            .build();

            responseObserver.onNext(runningProgress);
            responseObserver.onNext(completedProgress);
            responseObserver.onCompleted();
        }
    }
}