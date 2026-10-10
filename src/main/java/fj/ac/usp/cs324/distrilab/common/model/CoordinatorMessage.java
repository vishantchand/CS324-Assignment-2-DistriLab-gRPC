package fj.ac.usp.cs324.distrilab.common.model;

import java.io.Serializable;

public record CoordinatorMessage(
        String electionId,
        int coordinatorId,
        int coordinatorJac
) implements Serializable {

    private static final long serialVersionUID = 1L;

    public CoordinatorMessage {
        if (electionId == null || electionId.isBlank()) {
            throw new IllegalArgumentException("electionId cannot be blank");
        }
        if (coordinatorId < 0) {
            throw new IllegalArgumentException("coordinatorId cannot be negative");
        }
        if (coordinatorJac < 0) {
            throw new IllegalArgumentException("coordinatorJac cannot be negative");
        }
    }
}