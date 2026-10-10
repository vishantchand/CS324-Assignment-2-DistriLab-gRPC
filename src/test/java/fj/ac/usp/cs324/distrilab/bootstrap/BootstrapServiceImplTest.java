package fj.ac.usp.cs324.distrilab.bootstrap;

import fj.ac.usp.cs324.distrilab.common.model.WorkerInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BootstrapServiceImplTest {

    private BootstrapServiceImpl bootstrapService;
    private WorkerInfo workerOne;

    @BeforeEach
    void setUp() throws Exception {
        bootstrapService =
                new BootstrapServiceImpl();

        workerOne = new WorkerInfo(
                1,
                "localhost",
                1101,
                "Worker-1"
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        UnicastRemoteObject.unexportObject(
                bootstrapService,
                true
        );
    }

    @Test
    void registersWorkerSuccessfully()
            throws Exception {
        bootstrapService.registerWorker(workerOne);

        List<WorkerInfo> workers =
                bootstrapService.getActiveWorkers();

        assertEquals(1, workers.size());
        assertEquals(
                workerOne,
                workers.getFirst()
        );
    }

    @Test
    void registeringSameWorkerTwiceDoesNotCreateDuplicate()
            throws Exception {
        bootstrapService.registerWorker(workerOne);
        bootstrapService.registerWorker(workerOne);

        List<WorkerInfo> workers =
                bootstrapService.getActiveWorkers();

        assertEquals(1, workers.size());
        assertEquals(
                workerOne,
                workers.getFirst()
        );
    }

    @Test
    void rejectsDifferentWorkerWithDuplicateId()
            throws Exception {
        WorkerInfo conflictingWorker =
                new WorkerInfo(
                        1,
                        "localhost",
                        1201,
                        "Worker-1-Conflict"
                );

        bootstrapService.registerWorker(workerOne);

        assertThrows(
                IllegalArgumentException.class,
                () -> bootstrapService.registerWorker(
                        conflictingWorker
                )
        );

        assertEquals(
                1,
                bootstrapService
                        .getActiveWorkers()
                        .size()
        );
    }

    @Test
    void rejectsNullWorker() {
        assertThrows(
                IllegalArgumentException.class,
                () -> bootstrapService
                        .registerWorker(null)
        );
    }

    @Test
    void unregistersWorkerSuccessfully()
            throws Exception {
        bootstrapService.registerWorker(workerOne);

        bootstrapService.unregisterWorker(
                workerOne.workerId()
        );

        assertTrue(
                bootstrapService
                        .getActiveWorkers()
                        .isEmpty()
        );
    }

    @Test
    void returnsNullWhenNoWorkersAreRegistered()
            throws Exception {
        assertNull(
                bootstrapService.getRandomWorker()
        );
    }

    @Test
    void returnsARegisteredWorkerForDiscovery()
            throws Exception {
        WorkerInfo workerTwo =
                new WorkerInfo(
                        2,
                        "localhost",
                        1102,
                        "Worker-2"
                );

        bootstrapService.registerWorker(workerOne);
        bootstrapService.registerWorker(workerTwo);

        WorkerInfo selectedWorker =
                bootstrapService.getRandomWorker();

        assertTrue(
                selectedWorker.equals(workerOne)
                        || selectedWorker.equals(workerTwo)
        );
    }

    @Test
    void removesUnreachableWorkerFromActiveList()
            throws Exception {
        int unavailablePort;

        try (ServerSocket socket =
                     new ServerSocket(0)) {
            unavailablePort =
                    socket.getLocalPort();
        }

        WorkerInfo unreachableWorker =
                new WorkerInfo(
                        99,
                        "localhost",
                        unavailablePort,
                        "Worker-99"
                );

        bootstrapService.registerWorker(
                unreachableWorker
        );

        assertEquals(
                1,
                bootstrapService
                        .getActiveWorkers()
                        .size()
        );

        int removedWorkers =
                bootstrapService
                        .removeUnreachableWorkers();

        assertEquals(1, removedWorkers);

        assertTrue(
                bootstrapService
                        .getActiveWorkers()
                        .isEmpty()
        );
    }
}