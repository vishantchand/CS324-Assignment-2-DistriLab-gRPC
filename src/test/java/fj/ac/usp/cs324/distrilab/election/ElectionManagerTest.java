package fj.ac.usp.cs324.distrilab.election;

import fj.ac.usp.cs324.distrilab.common.model.CoordinatorMessage;
import fj.ac.usp.cs324.distrilab.common.model.ElectionMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ElectionManagerTest {

    @Test
    void startElectionCreatesUniqueElectionMessage() {
        ElectionManager manager = new ElectionManager(4, 2);

        ElectionMessage first = manager.startElection();
        ElectionMessage second = manager.startElection();

        assertNotNull(first.electionId());
        assertFalse(first.electionId().isBlank());
        assertNotEquals(first.electionId(), second.electionId());

        assertEquals(4, first.initiatorId());
        assertEquals(4, first.candidateId());
        assertEquals(2, first.candidateJac());
    }

    @Test
    void workerWithLowerJacBecomesCandidate() {
        ElectionManager manager = new ElectionManager(2, 3);

        ElectionMessage message =
                new ElectionMessage("election-1", 1, 1, 5);

        ElectionMessage result =
                manager.receiveElection(message);

        assertNotNull(result);
        assertEquals(2, result.candidateId());
        assertEquals(3, result.candidateJac());
    }

    @Test
    void candidateWithLowerJacRemainsCandidate() {
        ElectionManager manager = new ElectionManager(2, 8);

        ElectionMessage message =
                new ElectionMessage("election-2", 1, 1, 3);

        ElectionMessage result =
                manager.receiveElection(message);

        assertNotNull(result);
        assertEquals(1, result.candidateId());
        assertEquals(3, result.candidateJac());
    }

    @Test
    void higherWorkerIdWinsEqualJacTie() {
        boolean result = ElectionManager.isBetterCandidate(
                8,
                4,
                3,
                4
        );

        assertTrue(result);
    }

    @Test
    void duplicateElectionMessageIsIgnored() {
        ElectionManager manager = new ElectionManager(2, 3);

        ElectionMessage message =
                new ElectionMessage("duplicate-election", 1, 1, 5);

        ElectionMessage firstResult =
                manager.receiveElection(message);

        ElectionMessage duplicateResult =
                manager.receiveElection(message);

        assertNotNull(firstResult);
        assertNull(duplicateResult);
    }

    @Test
    void coordinatorInformationIsStored() {
        ElectionManager manager = new ElectionManager(2, 3);

        CoordinatorMessage message =
                new CoordinatorMessage("election-3", 7, 1);

        boolean accepted =
                manager.receiveCoordinator(message);

        assertTrue(accepted);
        assertTrue(manager.hasCoordinator());
        assertEquals(7, manager.getCoordinatorId());
        assertEquals(1, manager.getCoordinatorJac());
        assertFalse(manager.isCoordinator());
    }

    @Test
    void duplicateCoordinatorMessageIsIgnored() {
        ElectionManager manager = new ElectionManager(7, 1);

        CoordinatorMessage message =
                new CoordinatorMessage("election-4", 7, 1);

        assertTrue(manager.receiveCoordinator(message));
        assertFalse(manager.receiveCoordinator(message));
        assertTrue(manager.isCoordinator());
    }

    @Test
    void fifthAssignedJobTriggersNewElection() {
        ElectionManager manager = new ElectionManager(3, 0);

        assertFalse(manager.recordAssignedJob());
        assertFalse(manager.recordAssignedJob());
        assertFalse(manager.recordAssignedJob());
        assertFalse(manager.recordAssignedJob());
        assertTrue(manager.recordAssignedJob());

        assertEquals(5, manager.getJobsInCurrentTerm());
        assertEquals(5, manager.getJac());
    }

    @Test
    void coordinatorMessageIsCreatedFromElectionWinner() {
        ElectionManager manager = new ElectionManager(2, 3);

        ElectionMessage completedElection =
                new ElectionMessage("election-5", 1, 6, 2);

        CoordinatorMessage coordinatorMessage =
                manager.createCoordinatorMessage(completedElection);

        assertEquals(
                completedElection.electionId(),
                coordinatorMessage.electionId()
        );
        assertEquals(6, coordinatorMessage.coordinatorId());
        assertEquals(2, coordinatorMessage.coordinatorJac());
    }

    @Test
    void coordinatorWithNewerTermIsAccepted() {
        ElectionManager manager = new ElectionManager(2, 3);

        CoordinatorMessage message =
                new CoordinatorMessage(
                        "term-election-1",
                        7,
                        1
                );

        assertTrue(manager.receiveCoordinator(message, 2L));
        assertEquals(2L, manager.getCoordinatorTerm());
        assertEquals(7, manager.getCoordinatorId());
    }

    @Test
    void coordinatorWithStaleTermIsRejected() {
        ElectionManager manager = new ElectionManager(2, 3);

        CoordinatorMessage current =
                new CoordinatorMessage(
                        "term-election-2",
                        7,
                        1
                );

        CoordinatorMessage stale =
                new CoordinatorMessage(
                        "term-election-3",
                        4,
                        2
                );

        assertTrue(manager.receiveCoordinator(current, 3L));
        assertFalse(manager.receiveCoordinator(stale, 2L));

        assertEquals(3L, manager.getCoordinatorTerm());
        assertEquals(7, manager.getCoordinatorId());
    }

    @Test
    void electionRequiresTermNewerThanCurrentTerm() {
        ElectionManager manager = new ElectionManager(2, 3);

        CoordinatorMessage message =
                new CoordinatorMessage(
                        "term-election-4",
                        7,
                        1
                );

        assertTrue(manager.receiveCoordinator(message, 4L));

        assertFalse(manager.isValidElectionTerm(3L));
        assertFalse(manager.isValidElectionTerm(4L));
        assertTrue(manager.isValidElectionTerm(5L));
        assertEquals(5L, manager.getNextCoordinatorTerm());
    }
}