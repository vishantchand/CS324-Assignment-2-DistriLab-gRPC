package fj.ac.usp.cs324.distrilab.worker.job;

import fj.ac.usp.cs324.distrilab.grpc.JobRequest;
import fj.ac.usp.cs324.distrilab.grpc.JobSnapshot;
import fj.ac.usp.cs324.distrilab.grpc.TaskRequest;
import fj.ac.usp.cs324.distrilab.grpc.TaskSnapshot;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

public final class JobTaskCoordinator {

    private final JobTaskPartitioner partitioner;
    private final JobProgressTracker progressTracker;

    public JobTaskCoordinator() {
        this(
                new JobTaskPartitioner(),
                new JobProgressTracker()
        );
    }

    public JobTaskCoordinator(
            JobTaskPartitioner partitioner,
            JobProgressTracker progressTracker
    ) {
        this.partitioner = Objects.requireNonNull(
                partitioner,
                "Task partitioner cannot be null"
        );

        this.progressTracker = Objects.requireNonNull(
                progressTracker,
                "Progress tracker cannot be null"
        );
    }

    public synchronized List<TaskRequest> registerJob(
            JobRequest job,
            List<Integer> workerIds,
            int coordinatorId,
            long coordinatorTerm
    ) {
        Objects.requireNonNull(
                job,
                "Job request cannot be null"
        );

        if (progressTracker.containsJob(job.getJobId())) {
            return getExistingTasks(job.getJobId());
        }

        List<TaskRequest> tasks =
                partitioner.createTasks(
                        job,
                        workerIds,
                        coordinatorTerm
                );

        progressTracker.registerJob(
                job,
                tasks,
                coordinatorId,
                coordinatorTerm
        );

        return tasks;
    }

    public synchronized List<TaskRequest> reassignUnfinishedTasks(
            String jobId,
            int failedWorkerId,
            List<Integer> availableWorkerIds,
            int coordinatorId,
            long coordinatorTerm
    ) {
        JobSnapshot snapshot =
                progressTracker.getSnapshot(jobId);

        List<TaskSnapshot> unfinishedTasks =
                snapshot.getTasksList()
                        .stream()
                        .filter(task ->
                                task.getAssignedWorkerId()
                                        == failedWorkerId
                        )
                        .filter(task ->
                                !task.getResultRecorded()
                        )
                        .toList();

        if (unfinishedTasks.isEmpty()) {
            return List.of();
        }

        List<Integer> replacementWorkers =
                prepareReplacementWorkers(
                        availableWorkerIds,
                        failedWorkerId
                );

        if (replacementWorkers.isEmpty()) {
            throw new IllegalArgumentException(
                    "No reachable worker is available "
                            + "to reassign unfinished tasks"
            );
        }

        List<TaskRequest> reassignedTasks =
                new ArrayList<>();

        for (int index = 0;
             index < unfinishedTasks.size();
             index++) {

            TaskSnapshot taskSnapshot =
                    unfinishedTasks.get(index);

            TaskRequest oldRequest =
                    taskSnapshot.getRequest();

            int replacementWorkerId =
                    replacementWorkers.get(
                            index % replacementWorkers.size()
                    );

            int newAttempt;

            try {
                newAttempt = Math.addExact(
                        oldRequest.getAttempt(),
                        1
                );
            } catch (ArithmeticException exception) {
                throw new IllegalArgumentException(
                        "Task attempt number is too large",
                        exception
                );
            }

            progressTracker.reassignTask(
                    jobId,
                    oldRequest.getTaskId(),
                    replacementWorkerId,
                    newAttempt,
                    coordinatorId,
                    coordinatorTerm
            );

            TaskRequest reassignedRequest =
                    oldRequest.toBuilder()
                            .setAssignedWorkerId(
                                    replacementWorkerId
                            )
                            .setAttempt(newAttempt)
                            .setCoordinatorTerm(
                                    coordinatorTerm
                            )
                            .build();

            reassignedTasks.add(reassignedRequest);
        }

        return List.copyOf(reassignedTasks);
    }

    public JobProgressTracker getProgressTracker() {
        return progressTracker;
    }

    private List<TaskRequest> getExistingTasks(
            String jobId
    ) {
        return progressTracker.getSnapshot(jobId)
                .getTasksList()
                .stream()
                .map(TaskSnapshot::getRequest)
                .toList();
    }

    private List<Integer> prepareReplacementWorkers(
            List<Integer> workerIds,
            int failedWorkerId
    ) {
        Objects.requireNonNull(
                workerIds,
                "Worker ID list cannot be null"
        );

        LinkedHashSet<Integer> replacements =
                new LinkedHashSet<>();

        for (Integer workerId : workerIds) {
            if (workerId == null || workerId < 1) {
                throw new IllegalArgumentException(
                        "Worker IDs must be positive"
                );
            }

            if (workerId != failedWorkerId) {
                replacements.add(workerId);
            }
        }

        return List.copyOf(replacements);
    }
}