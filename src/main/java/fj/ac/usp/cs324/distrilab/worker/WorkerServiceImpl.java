package fj.ac.usp.cs324.distrilab.worker;

import fj.ac.usp.cs324.distrilab.common.model.CoordinatorMessage;
import fj.ac.usp.cs324.distrilab.common.model.ElectionMessage;
import fj.ac.usp.cs324.distrilab.common.model.JobRequest;
import fj.ac.usp.cs324.distrilab.common.model.JobResult;
import fj.ac.usp.cs324.distrilab.common.model.WorkerInfo;
import fj.ac.usp.cs324.distrilab.common.rmi.WorkerService;
import fj.ac.usp.cs324.distrilab.election.ElectionManager;
import fj.ac.usp.cs324.distrilab.worker.job.JobEngine;

import java.math.BigInteger;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class WorkerServiceImpl extends UnicastRemoteObject
        implements WorkerService {

    private static final long serialVersionUID = 1L;

    private final WorkerInfo workerInfo;
    private final ConcurrentHashMap<Integer, WorkerInfo> neighbors;
    private final ElectionManager electionManager;
    private final JobEngine jobEngine;
    private final AtomicInteger nextWorkerIndex;

    public WorkerServiceImpl(WorkerInfo workerInfo)
            throws RemoteException {
        this(workerInfo, 0);
    }

    public WorkerServiceImpl(
            WorkerInfo workerInfo,
            int initialJac
    ) throws RemoteException {
        super();

        this.workerInfo = Objects.requireNonNull(
                workerInfo,
                "workerInfo cannot be null"
        );

        this.neighbors = new ConcurrentHashMap<>();
        this.electionManager = new ElectionManager(
                workerInfo.workerId(),
                initialJac
        );
        this.jobEngine = new JobEngine();
        this.nextWorkerIndex = new AtomicInteger(0);
    }

    @Override
    public WorkerInfo getWorkerInfo() throws RemoteException {
        return workerInfo;
    }

    @Override
    public boolean ping() throws RemoteException {
        return true;
    }

    @Override
    public void addNeighbor(WorkerInfo worker)
            throws RemoteException {
        if (worker == null) {
            throw new IllegalArgumentException(
                    "worker cannot be null"
            );
        }

        if (worker.workerId() == workerInfo.workerId()) {
            return;
        }

        WorkerInfo previousWorker = neighbors.put(
                worker.workerId(),
                worker
        );

        if (!worker.equals(previousWorker)) {
            System.out.println("Added neighbor: " + worker);
        }
    }

    @Override
    public void removeNeighbor(int workerId)
            throws RemoteException {
        WorkerInfo removedWorker = neighbors.remove(workerId);

        if (removedWorker != null) {
            System.out.println(
                    "Removed neighbor: " + removedWorker
            );
        }
    }

    @Override
    public List<WorkerInfo> getNeighbors()
            throws RemoteException {
        return new ArrayList<>(neighbors.values());
    }

    public void removeUnreachableNeighbors() {
        List<WorkerInfo> neighborSnapshot =
                new ArrayList<>(neighbors.values());

        for (WorkerInfo neighbor : neighborSnapshot) {
            try {
                WorkerService neighborService =
                        lookupWorker(neighbor);

                if (!neighborService.ping()) {
                    removeUnreachableNeighbor(neighbor);
                }
            } catch (Exception exception) {
                removeUnreachableNeighbor(neighbor);
            }
        }
    }

    private void removeUnreachableNeighbor(
            WorkerInfo neighbor
    ) {
        boolean removed = neighbors.remove(
                neighbor.workerId(),
                neighbor
        );

        if (removed) {
            System.err.println(
                    "Removed unreachable neighbor: "
                            + neighbor.workerId()
            );
        }
    }

    @Override
    public synchronized void startElection()
            throws RemoteException {
        ElectionMessage bestCandidate =
                electionManager.startElection();

        List<WorkerInfo> neighborSnapshot =
                new ArrayList<>(neighbors.values());

        for (WorkerInfo neighbor : neighborSnapshot) {
            try {
                WorkerService neighborService =
                        lookupWorker(neighbor);

                ElectionMessage returnedCandidate =
                        neighborService.receiveElection(
                                bestCandidate
                        );

                bestCandidate = selectBetterMessage(
                        bestCandidate,
                        returnedCandidate
                );
            } catch (Exception exception) {
                removeUnreachableNeighbor(neighbor);
            }
        }

        CoordinatorMessage coordinatorMessage =
                electionManager.createCoordinatorMessage(
                        bestCandidate
                );

        receiveCoordinator(coordinatorMessage);
    }

    @Override
    public ElectionMessage receiveElection(
            ElectionMessage message
    ) throws RemoteException {
        ElectionMessage bestCandidate =
                electionManager.receiveElection(message);

        if (bestCandidate == null) {
            return message;
        }

        List<WorkerInfo> neighborSnapshot =
                new ArrayList<>(neighbors.values());

        for (WorkerInfo neighbor : neighborSnapshot) {
            try {
                WorkerService neighborService =
                        lookupWorker(neighbor);

                ElectionMessage returnedCandidate =
                        neighborService.receiveElection(
                                bestCandidate
                        );

                bestCandidate = selectBetterMessage(
                        bestCandidate,
                        returnedCandidate
                );
            } catch (Exception exception) {
                removeUnreachableNeighbor(neighbor);
            }
        }

        return bestCandidate;
    }

    private ElectionMessage selectBetterMessage(
            ElectionMessage first,
            ElectionMessage second
    ) {
        if (second == null) {
            return first;
        }

        boolean secondIsBetter =
                ElectionManager.isBetterCandidate(
                        second.candidateId(),
                        second.candidateJac(),
                        first.candidateId(),
                        first.candidateJac()
                );

        return secondIsBetter ? second : first;
    }

    @Override
    public void receiveCoordinator(
            CoordinatorMessage message
    ) throws RemoteException {
        boolean firstTimeReceived =
                electionManager.receiveCoordinator(message);

        if (!firstTimeReceived) {
            return;
        }

        System.out.println(
                "Worker " + workerInfo.workerId()
                        + " accepted coordinator "
                        + message.coordinatorId()
                        + " for election "
                        + message.electionId()
        );

        List<WorkerInfo> neighborSnapshot =
                new ArrayList<>(neighbors.values());

        for (WorkerInfo neighbor : neighborSnapshot) {
            try {
                WorkerService neighborService =
                        lookupWorker(neighbor);

                neighborService.receiveCoordinator(message);
            } catch (Exception exception) {
                removeUnreachableNeighbor(neighbor);
            }
        }
    }

    @Override
    public JobResult submitJob(JobRequest request)
            throws RemoteException {
        if (request == null) {
            throw new IllegalArgumentException(
                    "Job request cannot be null"
            );
        }

        if (!electionManager.hasCoordinator()) {
            startElection();
        }

        if (!electionManager.isCoordinator()) {
            ReachableWorker coordinator =
                    findReachableWorker(
                            electionManager.getCoordinatorId()
                    );

            if (coordinator == null) {
                startElection();

                if (!electionManager.isCoordinator()) {
                    coordinator = findReachableWorker(
                            electionManager.getCoordinatorId()
                    );
                }
            }

            if (coordinator == null) {
                throw new RemoteException(
                        "Coordinator is unreachable"
                );
            }

            return coordinator.service().submitJob(request);
        }

        return assignJob(request);
    }

    private JobResult assignJob(JobRequest request)
            throws RemoteException {
        List<ReachableWorker> executionWorkers =
                discoverReachableWorkers();

        executionWorkers.sort(
                Comparator.comparingInt(
                        worker -> worker.info().workerId()
                )
        );

        if (executionWorkers.isEmpty()) {
            executionWorkers.add(
                    new ReachableWorker(workerInfo, this)
            );
        }

        List<JobRequest> partitions =
                createPartitions(
                        request,
                        executionWorkers.size()
                );

        int startingIndex = Math.floorMod(
                nextWorkerIndex.getAndIncrement(),
                executionWorkers.size()
        );

        ExecutorService executor =
                Executors.newFixedThreadPool(
                        partitions.size()
                );

        List<Future<JobResult>> futures =
                new ArrayList<>();

        try {
            for (int index = 0;
                 index < partitions.size();
                 index++) {

                ReachableWorker selectedWorker =
                        executionWorkers.get(
                                (startingIndex + index)
                                        % executionWorkers.size()
                        );

                JobRequest partition =
                        partitions.get(index);

                futures.add(
                        executor.submit(
                                () -> executePartition(
                                        selectedWorker,
                                        partition
                                )
                        )
                );
            }

            List<JobResult> partialResults =
                    new ArrayList<>();

            for (Future<JobResult> future : futures) {
                try {
                    partialResults.add(future.get());
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();

                    throw new RemoteException(
                            "Job execution was interrupted",
                            exception
                    );
                } catch (ExecutionException exception) {
                    throw new RemoteException(
                            "A worker failed while executing a partition",
                            exception.getCause()
                    );
                }
            }

            JobResult combinedResult =
                    combineResults(
                            request,
                            partialResults
                    );

            recordAssignmentAndCheckTerm();

            return combinedResult;
        } finally {
            executor.shutdownNow();
        }
    }

    private JobResult executePartition(
            ReachableWorker selectedWorker,
            JobRequest partition
    ) throws RemoteException {
        boolean isRemoteWorker =
                selectedWorker.info().workerId()
                        != workerInfo.workerId();

        try {
            return selectedWorker.service()
                    .executeJob(partition);
        } catch (RemoteException exception) {
            if (!isRemoteWorker) {
                throw exception;
            }

            removeUnreachableNeighbor(
                    selectedWorker.info()
            );

            System.err.println(
                    "Worker "
                            + selectedWorker.info().workerId()
                            + " failed while executing "
                            + partition.jobId()
                            + "; coordinator will retry it"
            );

            return executeJob(partition);
        }
    }

    private List<JobRequest> createPartitions(
            JobRequest request,
            int workerCount
    ) {
        return switch (request.jobType()) {
            case MAX, PRIMECOUNT ->
                    partitionNumberList(
                            request,
                            workerCount
                    );

            case PRIMESUM ->
                    partitionRange(
                            request,
                            workerCount
                    );
        };
    }

    private List<JobRequest> partitionNumberList(
            JobRequest request,
            int workerCount
    ) {
        List<Long> numbers = request.numbers();

        if (numbers.isEmpty()) {
            return List.of(request);
        }

        int partitionCount = Math.min(
                workerCount,
                numbers.size()
        );

        int basicPartitionSize =
                numbers.size() / partitionCount;

        int remainingItems =
                numbers.size() % partitionCount;

        List<JobRequest> partitions =
                new ArrayList<>();

        int startingPosition = 0;

        for (int index = 0;
             index < partitionCount;
             index++) {

            int partitionSize =
                    basicPartitionSize
                            + (index < remainingItems ? 1 : 0);

            int endingPosition =
                    startingPosition + partitionSize;

            List<Long> partitionNumbers =
                    List.copyOf(
                            numbers.subList(
                                    startingPosition,
                                    endingPosition
                            )
                    );

            partitions.add(
                    new JobRequest(
                            request.jobId()
                                    + "-part-"
                                    + (index + 1),
                            request.jobType(),
                            partitionNumbers,
                            request.rangeStart(),
                            request.rangeEnd()
                    )
            );

            startingPosition = endingPosition;
        }

        return partitions;
    }

    private List<JobRequest> partitionRange(
            JobRequest request,
            int workerCount
    ) {
        if (request.rangeStart() > request.rangeEnd()) {
            return List.of(request);
        }

        BigInteger rangeStart =
                BigInteger.valueOf(
                        request.rangeStart()
                );

        BigInteger rangeEnd =
                BigInteger.valueOf(
                        request.rangeEnd()
                );

        BigInteger totalValues =
                rangeEnd.subtract(rangeStart)
                        .add(BigInteger.ONE);

        BigInteger workerCountValue =
                BigInteger.valueOf(workerCount);

        int partitionCount =
                totalValues.compareTo(workerCountValue) < 0
                        ? totalValues.intValueExact()
                        : workerCount;

        BigInteger[] division =
                totalValues.divideAndRemainder(
                        BigInteger.valueOf(partitionCount)
                );

        BigInteger basicPartitionSize =
                division[0];

        int remainingValues =
                division[1].intValueExact();

        List<JobRequest> partitions =
                new ArrayList<>();

        BigInteger currentStart = rangeStart;

        for (int index = 0;
             index < partitionCount;
             index++) {

            BigInteger partitionSize =
                    basicPartitionSize.add(
                            index < remainingValues
                                    ? BigInteger.ONE
                                    : BigInteger.ZERO
                    );

            BigInteger currentEnd =
                    currentStart.add(partitionSize)
                            .subtract(BigInteger.ONE);

            partitions.add(
                    new JobRequest(
                            request.jobId()
                                    + "-part-"
                                    + (index + 1),
                            request.jobType(),
                            List.of(),
                            currentStart.longValueExact(),
                            currentEnd.longValueExact()
                    )
            );

            currentStart =
                    currentEnd.add(BigInteger.ONE);
        }

        return partitions;
    }

    private JobResult combineResults(
            JobRequest originalRequest,
            List<JobResult> partialResults
    ) {
        for (JobResult partialResult : partialResults) {
            if (!partialResult.successful()) {
                return new JobResult(
                        originalRequest.jobId(),
                        originalRequest.jobType(),
                        0L,
                        false,
                        "Partition failed: "
                                + partialResult.message()
                );
            }
        }

        try {
            long finalValue =
                    switch (originalRequest.jobType()) {
                        case MAX -> partialResults.stream()
                                .mapToLong(JobResult::value)
                                .max()
                                .orElseThrow(
                                        () ->
                                                new IllegalArgumentException(
                                                        "No MAX results were returned"
                                                )
                                );

                        case PRIMESUM, PRIMECOUNT -> {
                            long total = 0L;

                            for (JobResult partialResult
                                    : partialResults) {
                                total = Math.addExact(
                                        total,
                                        partialResult.value()
                                );
                            }

                            yield total;
                        }
                    };

            return new JobResult(
                    originalRequest.jobId(),
                    originalRequest.jobType(),
                    finalValue,
                    true,
                    "Job completed successfully using "
                            + partialResults.size()
                            + " worker partition(s)"
            );
        } catch (IllegalArgumentException
                 | ArithmeticException exception) {
            return new JobResult(
                    originalRequest.jobId(),
                    originalRequest.jobType(),
                    0L,
                    false,
                    exception.getMessage()
            );
        }
    }

    private void recordAssignmentAndCheckTerm()
            throws RemoteException {
        boolean termCompleted =
                electionManager.recordAssignedJob();

        if (termCompleted) {
            System.out.println(
                    "Coordinator "
                            + workerInfo.workerId()
                            + " completed five jobs; "
                            + "starting a new election"
            );

            startElection();
        }
    }

    @Override
    public JobResult executeJob(JobRequest request)
            throws RemoteException {
        return jobEngine.executeJob(request);
    }

    private ReachableWorker findReachableWorker(
            int requiredWorkerId
    ) {
        for (ReachableWorker reachableWorker
                : discoverReachableWorkers()) {
            if (reachableWorker.info().workerId()
                    == requiredWorkerId) {
                return reachableWorker;
            }
        }

        return null;
    }

    private List<ReachableWorker>
    discoverReachableWorkers() {
        Map<Integer, ReachableWorker> discovered =
                new HashMap<>();

        Set<Integer> visitedWorkerIds =
                new HashSet<>();

        ArrayDeque<WorkerInfo> pendingWorkers =
                new ArrayDeque<>();

        pendingWorkers.add(workerInfo);
        pendingWorkers.addAll(neighbors.values());

        while (!pendingWorkers.isEmpty()) {
            WorkerInfo currentWorker =
                    pendingWorkers.removeFirst();

            if (!visitedWorkerIds.add(
                    currentWorker.workerId()
            )) {
                continue;
            }

            WorkerService currentService;

            if (currentWorker.workerId()
                    == workerInfo.workerId()) {
                currentService = this;
            } else {
                try {
                    currentService =
                            lookupWorker(currentWorker);

                    if (!currentService.ping()) {
                        removeUnreachableNeighbor(
                                currentWorker
                        );
                        continue;
                    }
                } catch (Exception exception) {
                    removeUnreachableNeighbor(
                            currentWorker
                    );
                    continue;
                }
            }

            discovered.put(
                    currentWorker.workerId(),
                    new ReachableWorker(
                            currentWorker,
                            currentService
                    )
            );

            try {
                pendingWorkers.addAll(
                        currentService.getNeighbors()
                );
            } catch (RemoteException exception) {
                if (currentWorker.workerId()
                        != workerInfo.workerId()) {
                    removeUnreachableNeighbor(
                            currentWorker
                    );
                }
            }
        }

        return new ArrayList<>(
                discovered.values()
        );
    }

    private WorkerService lookupWorker(
            WorkerInfo worker
    ) throws RemoteException, NotBoundException {
        Registry registry = LocateRegistry.getRegistry(
                worker.host(),
                worker.registryPort()
        );

        return (WorkerService) registry.lookup(
                worker.bindingName()
        );
    }

    public int getJac() {
        return electionManager.getJac();
    }

    public int getCoordinatorId() {
        return electionManager.getCoordinatorId();
    }

    public int getJobsInCurrentTerm() {
        return electionManager.getJobsInCurrentTerm();
    }

    public boolean isCoordinator() {
        return electionManager.isCoordinator();
    }

    private record ReachableWorker(
            WorkerInfo info,
            WorkerService service
    ) {
    }
}