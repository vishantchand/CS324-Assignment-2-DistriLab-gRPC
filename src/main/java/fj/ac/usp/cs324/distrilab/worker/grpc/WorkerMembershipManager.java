package fj.ac.usp.cs324.distrilab.worker.grpc;

import fj.ac.usp.cs324.distrilab.grpc.WorkerInfo;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class WorkerMembershipManager {

    private final int localWorkerId;

    private final ConcurrentMap<Integer, WorkerInfo> neighbours =
            new ConcurrentHashMap<>();

    public WorkerMembershipManager(int localWorkerId) {
        if (localWorkerId <= 0) {
            throw new IllegalArgumentException(
                    "localWorkerId must be greater than zero"
            );
        }

        this.localWorkerId = localWorkerId;
    }

    public boolean addNeighbour(WorkerInfo worker) {
        validateWorker(worker);

        if (worker.getWorkerId() == localWorkerId) {
            return false;
        }

        WorkerInfo previous = neighbours.put(
                worker.getWorkerId(),
                worker
        );

        return !worker.equals(previous);
    }

    public Optional<WorkerInfo> removeNeighbour(int workerId) {
        if (workerId <= 0) {
            return Optional.empty();
        }

        return Optional.ofNullable(
                neighbours.remove(workerId)
        );
    }

    public Optional<WorkerInfo> findNeighbour(int workerId) {
        return Optional.ofNullable(
                neighbours.get(workerId)
        );
    }

    public boolean containsNeighbour(int workerId) {
        return neighbours.containsKey(workerId);
    }

    public List<WorkerInfo> getNeighboursSnapshot() {
        List<WorkerInfo> snapshot =
                new ArrayList<>(neighbours.values());

        snapshot.sort(
                Comparator.comparingInt(WorkerInfo::getWorkerId)
        );

        return List.copyOf(snapshot);
    }

    public int size() {
        return neighbours.size();
    }

    public boolean isEmpty() {
        return neighbours.isEmpty();
    }

    public void clear() {
        neighbours.clear();
    }

    private static void validateWorker(WorkerInfo worker) {
        Objects.requireNonNull(
                worker,
                "worker cannot be null"
        );

        if (worker.getWorkerId() <= 0) {
            throw new IllegalArgumentException(
                    "worker ID must be greater than zero"
            );
        }

        if (worker.getHost().isBlank()) {
            throw new IllegalArgumentException(
                    "worker host cannot be blank"
            );
        }

        if (worker.getGrpcPort() < 1
                || worker.getGrpcPort() > 65_535) {
            throw new IllegalArgumentException(
                    "gRPC port must be between 1 and 65535"
            );
        }
    }
}