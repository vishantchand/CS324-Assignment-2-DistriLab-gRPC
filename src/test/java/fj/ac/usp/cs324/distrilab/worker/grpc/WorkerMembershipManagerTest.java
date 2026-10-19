package fj.ac.usp.cs324.distrilab.worker.grpc;

import fj.ac.usp.cs324.distrilab.grpc.WorkerInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerMembershipManagerTest {

    private WorkerMembershipManager membershipManager;

    @BeforeEach
    void setUp() {
        membershipManager = new WorkerMembershipManager(1);
    }

    @Test
    void addsNeighbour() {
        WorkerInfo workerTwo = worker(2, 5002);

        assertTrue(membershipManager.addNeighbour(workerTwo));
        assertEquals(1, membershipManager.size());
        assertEquals(
                workerTwo,
                membershipManager.findNeighbour(2).orElseThrow()
        );
    }

    @Test
    void ignoresLocalWorker() {
        WorkerInfo localWorker = worker(1, 5001);

        assertFalse(
                membershipManager.addNeighbour(localWorker)
        );
        assertTrue(membershipManager.isEmpty());
    }

    @Test
    void repeatedIdenticalAdditionIsIdempotent() {
        WorkerInfo workerTwo = worker(2, 5002);

        assertTrue(membershipManager.addNeighbour(workerTwo));
        assertFalse(membershipManager.addNeighbour(workerTwo));
        assertEquals(1, membershipManager.size());
    }

    @Test
    void updatesChangedWorkerInformation() {
        WorkerInfo original = worker(2, 5002);
        WorkerInfo updated = worker(2, 6002);

        membershipManager.addNeighbour(original);

        assertTrue(membershipManager.addNeighbour(updated));
        assertEquals(
                6002,
                membershipManager.findNeighbour(2)
                        .orElseThrow()
                        .getGrpcPort()
        );
    }

    @Test
    void removesNeighbour() {
        membershipManager.addNeighbour(worker(2, 5002));

        assertTrue(
                membershipManager.removeNeighbour(2).isPresent()
        );
        assertFalse(membershipManager.containsNeighbour(2));
    }

    @Test
    void returnsSortedImmutableSnapshot() {
        membershipManager.addNeighbour(worker(3, 5003));
        membershipManager.addNeighbour(worker(2, 5002));

        List<WorkerInfo> snapshot =
                membershipManager.getNeighboursSnapshot();

        assertEquals(2, snapshot.get(0).getWorkerId());
        assertEquals(3, snapshot.get(1).getWorkerId());

        assertThrows(
                UnsupportedOperationException.class,
                () -> snapshot.add(worker(4, 5004))
        );
    }

    @Test
    void rejectsInvalidWorker() {
        WorkerInfo invalidWorker = WorkerInfo.newBuilder()
                .setWorkerId(2)
                .setHost("")
                .setGrpcPort(5002)
                .setWorkerName("Worker-2")
                .build();

        assertThrows(
                IllegalArgumentException.class,
                () -> membershipManager.addNeighbour(invalidWorker)
        );
    }

    private static WorkerInfo worker(int workerId, int port) {
        return WorkerInfo.newBuilder()
                .setWorkerId(workerId)
                .setHost("localhost")
                .setGrpcPort(port)
                .setWorkerName("Worker-" + workerId)
                .build();
    }
}