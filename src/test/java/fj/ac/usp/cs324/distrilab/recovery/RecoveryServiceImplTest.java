package fj.ac.usp.cs324.distrilab.recovery;

import fj.ac.usp.cs324.distrilab.grpc.CoordinatorStateReply;
import fj.ac.usp.cs324.distrilab.grpc.CoordinatorStateSnapshot;
import fj.ac.usp.cs324.distrilab.grpc.Empty;
import fj.ac.usp.cs324.distrilab.grpc.JobRequest;
import fj.ac.usp.cs324.distrilab.grpc.JobSnapshot;
import fj.ac.usp.cs324.distrilab.grpc.JobState;
import fj.ac.usp.cs324.distrilab.grpc.JobType;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RecoveryServiceImplTest {

    @Test
    void storesAndReturnsCoordinatorSnapshot() {
        RecoveryServiceImpl service =
                new RecoveryServiceImpl();

        CoordinatorStateSnapshot snapshot =
                createSnapshot(4, 1, 1)
                        .toBuilder()
                        .addJobs(
                                JobSnapshot.newBuilder()
                                        .setRequest(
                                                JobRequest.newBuilder()
                                                        .setJobId("job-1")
                                                        .setJobType(
                                                                JobType.JOB_TYPE_MAX
                                                        )
                                                        .addNumbers(4)
                                                        .addNumbers(8)
                                                        .build()
                                        )
                                        .setState(
                                                JobState.JOB_STATE_RUNNING
                                        )
                                        .setCompletedWorkUnits(1)
                                        .setTotalWorkUnits(2)
                                        .build()
                        )
                        .build();

        TestObserver<CoordinatorStateReply>
                replicationObserver = new TestObserver<>();

        service.replicateCoordinatorState(
                snapshot,
                replicationObserver
        );

        assertNull(replicationObserver.error);
        assertTrue(replicationObserver.completed);
        assertNotNull(replicationObserver.value);
        assertTrue(
                replicationObserver.value.getAccepted()
        );
        assertEquals(
                1,
                replicationObserver.value.getStoredVersion()
        );

        TestObserver<CoordinatorStateSnapshot>
                stateObserver = new TestObserver<>();

        service.getCoordinatorState(
                Empty.getDefaultInstance(),
                stateObserver
        );

        assertNull(stateObserver.error);
        assertTrue(stateObserver.completed);
        assertEquals(snapshot, stateObserver.value);
        assertEquals(1, stateObserver.value.getJobsCount());
        assertEquals(
                "job-1",
                stateObserver.value
                        .getJobs(0)
                        .getRequest()
                        .getJobId()
        );
    }

    @Test
    void rejectsOlderVersionFromSameTerm() {
        RecoveryServiceImpl service =
                new RecoveryServiceImpl();

        TestObserver<CoordinatorStateReply> firstObserver =
                new TestObserver<>();

        service.replicateCoordinatorState(
                createSnapshot(4, 1, 5),
                firstObserver
        );

        assertTrue(firstObserver.value.getAccepted());

        TestObserver<CoordinatorStateReply> oldObserver =
                new TestObserver<>();

        service.replicateCoordinatorState(
                createSnapshot(4, 1, 4),
                oldObserver
        );

        assertNull(oldObserver.error);
        assertTrue(oldObserver.completed);
        assertFalse(oldObserver.value.getAccepted());
        assertEquals(5, oldObserver.value.getStoredVersion());
        assertEquals(
                "Snapshot version is not newer",
                oldObserver.value.getMessage()
        );
    }

    @Test
    void acceptsLowerVersionFromNewerTerm() {
        RecoveryServiceImpl service =
                new RecoveryServiceImpl();

        TestObserver<CoordinatorStateReply> firstObserver =
                new TestObserver<>();

        service.replicateCoordinatorState(
                createSnapshot(4, 1, 5),
                firstObserver
        );

        TestObserver<CoordinatorStateReply> newerObserver =
                new TestObserver<>();

        service.replicateCoordinatorState(
                createSnapshot(7, 2, 1),
                newerObserver
        );

        assertNull(newerObserver.error);
        assertTrue(newerObserver.completed);
        assertTrue(newerObserver.value.getAccepted());
        assertEquals(1, newerObserver.value.getStoredVersion());

        TestObserver<CoordinatorStateSnapshot>
                stateObserver = new TestObserver<>();

        service.getCoordinatorState(
                Empty.getDefaultInstance(),
                stateObserver
        );

        assertEquals(7, stateObserver.value.getCoordinatorId());
        assertEquals(2, stateObserver.value.getCoordinatorTerm());
        assertEquals(1, stateObserver.value.getVersion());
    }

    @Test
    void rejectsSnapshotFromOlderTerm() {
        RecoveryServiceImpl service =
                new RecoveryServiceImpl();

        TestObserver<CoordinatorStateReply> firstObserver =
                new TestObserver<>();

        service.replicateCoordinatorState(
                createSnapshot(7, 3, 2),
                firstObserver
        );

        TestObserver<CoordinatorStateReply> oldObserver =
                new TestObserver<>();

        service.replicateCoordinatorState(
                createSnapshot(4, 2, 10),
                oldObserver
        );

        assertNull(oldObserver.error);
        assertTrue(oldObserver.completed);
        assertFalse(oldObserver.value.getAccepted());
        assertEquals(2, oldObserver.value.getStoredVersion());
        assertEquals(
                "Snapshot has an older coordinator term",
                oldObserver.value.getMessage()
        );
    }

    private CoordinatorStateSnapshot createSnapshot(
            int coordinatorId,
            long coordinatorTerm,
            long version
    ) {
        return CoordinatorStateSnapshot.newBuilder()
                .setCoordinatorId(coordinatorId)
                .setCoordinatorTerm(coordinatorTerm)
                .setVersion(version)
                .setCreatedAtMs(System.currentTimeMillis())
                .build();
    }

    private static final class TestObserver<T>
            implements StreamObserver<T> {

        private T value;
        private Throwable error;
        private boolean completed;

        @Override
        public void onNext(T value) {
            this.value = value;
        }

        @Override
        public void onError(Throwable error) {
            this.error = error;
        }

        @Override
        public void onCompleted() {
            this.completed = true;
        }
    }
}