package fj.ac.usp.cs324.distrilab.common.model;

import java.io.Serializable;

public record WorkerInfo(
        int workerId,
        String host,
        int registryPort,
        String bindingName
) implements Serializable {

    private static final long serialVersionUID = 1L;

    public WorkerInfo {
        if (workerId < 0) {
            throw new IllegalArgumentException("workerId cannot be negative");
        }
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host cannot be blank");
        }
        if (registryPort < 1 || registryPort > 65535) {
            throw new IllegalArgumentException("registryPort must be between 1 and 65535");
        }
        if (bindingName == null || bindingName.isBlank()) {
            throw new IllegalArgumentException("bindingName cannot be blank");
        }
    }
}