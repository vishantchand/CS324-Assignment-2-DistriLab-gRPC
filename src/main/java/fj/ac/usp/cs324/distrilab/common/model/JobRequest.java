package fj.ac.usp.cs324.distrilab.common.model;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public record JobRequest(
        String jobId,
        JobType jobType,
        List<Long> numbers,
        long rangeStart,
        long rangeEnd
) implements Serializable {

    private static final long serialVersionUID = 1L;

    public JobRequest {
        Objects.requireNonNull(jobId, "jobId cannot be null");
        Objects.requireNonNull(jobType, "jobType cannot be null");
        numbers = numbers == null ? List.of() : List.copyOf(numbers);
    }
}