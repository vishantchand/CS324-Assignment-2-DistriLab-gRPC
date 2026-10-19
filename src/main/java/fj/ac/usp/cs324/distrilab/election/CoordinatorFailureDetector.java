package fj.ac.usp.cs324.distrilab.election;

import java.util.function.LongSupplier;

public final class CoordinatorFailureDetector {

    private final long timeoutMs;
    private final Runnable electionStarter;
    private final LongSupplier currentTimeSupplier;

    private long lastHeartbeatTimeMs;
    private boolean electionTriggered;

    public CoordinatorFailureDetector(
            long timeoutMs,
            Runnable electionStarter
    ) {
        this(
                timeoutMs,
                electionStarter,
                System::currentTimeMillis
        );
    }

    CoordinatorFailureDetector(
            long timeoutMs,
            Runnable electionStarter,
            LongSupplier currentTimeSupplier
    ) {
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException(
                    "timeoutMs must be greater than zero"
            );
        }

        if (electionStarter == null) {
            throw new IllegalArgumentException(
                    "electionStarter cannot be null"
            );
        }

        if (currentTimeSupplier == null) {
            throw new IllegalArgumentException(
                    "currentTimeSupplier cannot be null"
            );
        }

        this.timeoutMs = timeoutMs;
        this.electionStarter = electionStarter;
        this.currentTimeSupplier = currentTimeSupplier;
        this.lastHeartbeatTimeMs = currentTimeSupplier.getAsLong();
    }

    public synchronized void recordHeartbeat() {
        lastHeartbeatTimeMs = currentTimeSupplier.getAsLong();
        electionTriggered = false;
    }

    public synchronized boolean checkCoordinator() {
        long elapsedTime =
                currentTimeSupplier.getAsLong()
                        - lastHeartbeatTimeMs;

        if (elapsedTime >= timeoutMs && !electionTriggered) {
            electionTriggered = true;
            electionStarter.run();
            return false;
        }

        return elapsedTime < timeoutMs;
    }

    public synchronized long getLastHeartbeatTimeMs() {
        return lastHeartbeatTimeMs;
    }

    public synchronized boolean hasTriggeredElection() {
        return electionTriggered;
    }
}