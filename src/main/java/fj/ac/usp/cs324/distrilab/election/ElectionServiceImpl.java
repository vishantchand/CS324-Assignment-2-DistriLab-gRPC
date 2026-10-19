package fj.ac.usp.cs324.distrilab.election;

import fj.ac.usp.cs324.distrilab.common.model.CoordinatorMessage;
import fj.ac.usp.cs324.distrilab.common.model.ElectionMessage;
import fj.ac.usp.cs324.distrilab.grpc.Acknowledgement;
import fj.ac.usp.cs324.distrilab.grpc.CoordinatorAnnouncement;
import fj.ac.usp.cs324.distrilab.grpc.ElectionReply;
import fj.ac.usp.cs324.distrilab.grpc.ElectionRequest;
import fj.ac.usp.cs324.distrilab.grpc.ElectionServiceGrpc;
import fj.ac.usp.cs324.distrilab.grpc.HeartbeatReply;
import fj.ac.usp.cs324.distrilab.grpc.HeartbeatRequest;
import fj.ac.usp.cs324.distrilab.grpc.WorkerHealth;
import fj.ac.usp.cs324.distrilab.grpc.WorkerInfo;
import io.grpc.stub.StreamObserver;

public final class ElectionServiceImpl
        extends ElectionServiceGrpc.ElectionServiceImplBase {

    private final ElectionManager electionManager;
    private final WorkerInfo workerInfo;

    public ElectionServiceImpl(
            ElectionManager electionManager,
            WorkerInfo workerInfo
    ) {
        if (electionManager == null) {
            throw new IllegalArgumentException(
                    "electionManager cannot be null"
            );
        }

        if (workerInfo == null) {
            throw new IllegalArgumentException(
                    "workerInfo cannot be null"
            );
        }

        this.electionManager = electionManager;
        this.workerInfo = workerInfo;
    }

    @Override
    public void receiveElection(
            ElectionRequest request,
            StreamObserver<ElectionReply> responseObserver
    ) {
        if (!electionManager.isValidElectionTerm(
                request.getProposedTerm()
        )) {
            ElectionReply reply = ElectionReply.newBuilder()
                    .setAccepted(false)
                    .setElectionId(request.getElectionId())
                    .setCandidateId(request.getCandidateId())
                    .setCandidateJac(request.getCandidateJac())
                    .setProposedTerm(
                            electionManager.getCoordinatorTerm()
                    )
                    .setMessage("Election term is stale")
                    .build();

            responseObserver.onNext(reply);
            responseObserver.onCompleted();
            return;
        }

        ElectionMessage electionMessage = new ElectionMessage(
                request.getElectionId(),
                request.getInitiatorId(),
                request.getCandidateId(),
                Math.toIntExact(request.getCandidateJac())
        );

        ElectionMessage selectedCandidate =
                electionManager.receiveElection(electionMessage);

        if (selectedCandidate == null) {
            ElectionReply reply = ElectionReply.newBuilder()
                    .setAccepted(false)
                    .setElectionId(request.getElectionId())
                    .setCandidateId(request.getCandidateId())
                    .setCandidateJac(request.getCandidateJac())
                    .setProposedTerm(request.getProposedTerm())
                    .setMessage("Duplicate election ignored")
                    .build();

            responseObserver.onNext(reply);
            responseObserver.onCompleted();
            return;
        }

        ElectionReply reply = ElectionReply.newBuilder()
                .setAccepted(true)
                .setElectionId(selectedCandidate.electionId())
                .setCandidateId(selectedCandidate.candidateId())
                .setCandidateJac(selectedCandidate.candidateJac())
                .setProposedTerm(request.getProposedTerm())
                .setMessage("Election request accepted")
                .build();

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void announceCoordinator(
            CoordinatorAnnouncement request,
            StreamObserver<Acknowledgement> responseObserver
    ) {
        if (!electionManager.isValidElectionTerm(
                request.getCoordinatorTerm()
        )) {
            Acknowledgement reply = Acknowledgement.newBuilder()
                    .setSuccess(false)
                    .setMessage(
                            "Coordinator announcement has a stale term"
                    )
                    .build();

            responseObserver.onNext(reply);
            responseObserver.onCompleted();
            return;
        }

        CoordinatorMessage coordinatorMessage =
                new CoordinatorMessage(
                        request.getElectionId(),
                        request.getCoordinatorId(),
                        Math.toIntExact(
                                request.getCoordinatorJac()
                        )
                );

        boolean accepted = electionManager.receiveCoordinator(
                coordinatorMessage,
                request.getCoordinatorTerm()
        );

        Acknowledgement reply = Acknowledgement.newBuilder()
                .setSuccess(accepted)
                .setMessage(
                        accepted
                                ? "Coordinator announcement accepted"
                                : "Duplicate coordinator announcement ignored"
                )
                .build();

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void coordinatorHeartbeat(
            HeartbeatRequest request,
            StreamObserver<HeartbeatReply> responseObserver
    ) {
        boolean coordinatorMatches =
                electionManager.isCoordinator()
                        && request.getKnownCoordinatorId()
                        == electionManager.getCoordinatorId()
                        && request.getCoordinatorTerm()
                        == electionManager.getCoordinatorTerm();

        HeartbeatReply reply = HeartbeatReply.newBuilder()
                .setAlive(coordinatorMatches)
                .setWorker(workerInfo)
                .setHealth(
                        coordinatorMatches
                                ? WorkerHealth.WORKER_HEALTH_ACTIVE
                                : WorkerHealth.WORKER_HEALTH_SUSPECTED
                )
                .setCoordinatorId(
                        electionManager.getCoordinatorId()
                )
                .setCoordinatorTerm(
                        electionManager.getCoordinatorTerm()
                )
                .setTimestampMs(System.currentTimeMillis())
                .build();

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }
}