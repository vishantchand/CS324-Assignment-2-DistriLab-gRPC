package fj.ac.usp.cs324.distrilab.election;

import fj.ac.usp.cs324.distrilab.common.model.CoordinatorMessage;
import fj.ac.usp.cs324.distrilab.common.model.ElectionMessage;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ElectionManager {

    private static final int NO_COORDINATOR = -1;
    private static final int MAX_JOBS_PER_TERM = 5;

    private final int workerId;
    private final AtomicInteger jac;

    private final Set<String> processedElectionIds =
            ConcurrentHashMap.newKeySet();

    private final Set<String> processedCoordinatorMessages =
            ConcurrentHashMap.newKeySet();

    private final AtomicInteger coordinatorId =
            new AtomicInteger(NO_COORDINATOR);

    private final AtomicInteger coordinatorJac =
            new AtomicInteger(0);

    private final AtomicLong coordinatorTerm =
            new AtomicLong(0L);

    private final AtomicInteger jobsInCurrentTerm =
            new AtomicInteger(0);

    public ElectionManager(int workerId, int initialJac) {
        if (workerId < 0) {
            throw new IllegalArgumentException(
                    "workerId cannot be negative"
            );
        }

        if (initialJac < 0) {
            throw new IllegalArgumentException(
                    "initialJac cannot be negative"
            );
        }

        this.workerId = workerId;
        this.jac = new AtomicInteger(initialJac);
    }

    public ElectionMessage startElection() {
        String electionId = UUID.randomUUID().toString();

        processedElectionIds.add(electionId);

        return new ElectionMessage(
                electionId,
                workerId,
                workerId,
                jac.get()
        );
    }

    public ElectionMessage receiveElection(ElectionMessage message) {
        if (message == null) {
            throw new IllegalArgumentException(
                    "message cannot be null"
            );
        }

        boolean firstTimeReceived =
                processedElectionIds.add(message.electionId());

        if (!firstTimeReceived) {
            return null;
        }

        return chooseBestCandidate(message);
    }

    public ElectionMessage chooseBestCandidate(
            ElectionMessage message
    ) {
        if (message == null) {
            throw new IllegalArgumentException(
                    "message cannot be null"
            );
        }

        int localJac = jac.get();

        if (isBetterCandidate(
                workerId,
                localJac,
                message.candidateId(),
                message.candidateJac()
        )) {
            return new ElectionMessage(
                    message.electionId(),
                    message.initiatorId(),
                    workerId,
                    localJac
            );
        }

        return message;
    }

    public CoordinatorMessage createCoordinatorMessage(
            ElectionMessage completedElection
    ) {
        if (completedElection == null) {
            throw new IllegalArgumentException(
                    "completedElection cannot be null"
            );
        }

        return new CoordinatorMessage(
                completedElection.electionId(),
                completedElection.candidateId(),
                completedElection.candidateJac()
        );
    }

    public synchronized boolean receiveCoordinator(
            CoordinatorMessage message
    ) {
        return receiveCoordinator(
                message,
                coordinatorTerm.get() + 1
        );
    }

    public synchronized boolean receiveCoordinator(
            CoordinatorMessage message,
            long term
    ) {
        if (message == null) {
            throw new IllegalArgumentException(
                    "message cannot be null"
            );
        }

        if (term <= 0) {
            throw new IllegalArgumentException(
                    "coordinator term must be positive"
            );
        }

        if (term <= coordinatorTerm.get()) {
            return false;
        }

        boolean firstTimeReceived =
                processedCoordinatorMessages.add(
                        message.electionId()
                );

        if (!firstTimeReceived) {
            return false;
        }

        coordinatorId.set(message.coordinatorId());
        coordinatorJac.set(message.coordinatorJac());
        coordinatorTerm.set(term);
        jobsInCurrentTerm.set(0);

        return true;
    }

    public boolean recordAssignedJob() {
        jac.incrementAndGet();

        int assignedJobs =
                jobsInCurrentTerm.incrementAndGet();

        return assignedJobs == MAX_JOBS_PER_TERM;
    }

    public int getWorkerId() {
        return workerId;
    }

    public int getJac() {
        return jac.get();
    }

    public int getCoordinatorId() {
        return coordinatorId.get();
    }

    public int getCoordinatorJac() {
        return coordinatorJac.get();
    }

    public long getCoordinatorTerm() {
        return coordinatorTerm.get();
    }

    public long getNextCoordinatorTerm() {
        return coordinatorTerm.get() + 1;
    }

    public boolean isValidElectionTerm(long proposedTerm) {
        return proposedTerm > coordinatorTerm.get();
    }

    public int getJobsInCurrentTerm() {
        return jobsInCurrentTerm.get();
    }

    public boolean hasCoordinator() {
        return coordinatorId.get() != NO_COORDINATOR;
    }

    public boolean isCoordinator() {
        return coordinatorId.get() == workerId;
    }

    public static boolean isBetterCandidate(
            int firstWorkerId,
            int firstJac,
            int secondWorkerId,
            int secondJac
    ) {
        if (firstJac != secondJac) {
            return firstJac < secondJac;
        }

        return firstWorkerId > secondWorkerId;
    }
}