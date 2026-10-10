package fj.ac.usp.cs324.distrilab.common.model;

import java.io.Serializable;
import java.util.Objects;

public record JobResult(
        String jobId,
        JobType jobType,
        long value,
        boolean successful,
        String message
) implements Serializable {

    private static final long serialVersionUID = 1L;

    public JobResult {
        Objects.requireNonNull(jobId, "jobId cannot be null");
        Objects.requireNonNull(jobType, "jobType cannot be null");
        message = message == null ? "" : message;
    }
}