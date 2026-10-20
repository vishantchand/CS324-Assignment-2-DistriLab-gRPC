package fj.ac.usp.cs324.distrilab.recovery;

import fj.ac.usp.cs324.distrilab.grpc.CoordinatorStateReply;
import fj.ac.usp.cs324.distrilab.grpc.CoordinatorStateSnapshot;
import fj.ac.usp.cs324.distrilab.grpc.Empty;
import fj.ac.usp.cs324.distrilab.grpc.RecoveryServiceGrpc;
import io.grpc.stub.StreamObserver;

public final class RecoveryServiceImpl
        extends RecoveryServiceGrpc.RecoveryServiceImplBase {

    private CoordinatorStateSnapshot latestSnapshot =
            CoordinatorStateSnapshot.newBuilder()
                    .setCoordinatorId(-1)
                    .setCoordinatorTerm(0)
                    .setVersion(0)
                    .setCreatedAtMs(0)
                    .build();

    @Override
    public synchronized void replicateCoordinatorState(
            CoordinatorStateSnapshot request,
            StreamObserver<CoordinatorStateReply> responseObserver
    ) {
        if (request.getCoordinatorTerm()
                < latestSnapshot.getCoordinatorTerm()) {
            sendReply(
                    false,
                    "Snapshot has an older coordinator term",
                    responseObserver
            );
            return;
        }

        boolean sameTerm =
                request.getCoordinatorTerm()
                        == latestSnapshot.getCoordinatorTerm();

        if (sameTerm
                && request.getVersion()
                <= latestSnapshot.getVersion()) {
            sendReply(
                    false,
                    "Snapshot version is not newer",
                    responseObserver
            );
            return;
        }

        latestSnapshot = request;

        sendReply(
                true,
                "Coordinator state snapshot stored",
                responseObserver
        );
    }

    @Override
    public synchronized void getCoordinatorState(
            Empty request,
            StreamObserver<CoordinatorStateSnapshot> responseObserver
    ) {
        responseObserver.onNext(latestSnapshot);
        responseObserver.onCompleted();
    }

    private void sendReply(
            boolean accepted,
            String message,
            StreamObserver<CoordinatorStateReply> responseObserver
    ) {
        CoordinatorStateReply reply =
                CoordinatorStateReply.newBuilder()
                        .setAccepted(accepted)
                        .setStoredVersion(
                                latestSnapshot.getVersion()
                        )
                        .setMessage(message)
                        .build();

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }
}