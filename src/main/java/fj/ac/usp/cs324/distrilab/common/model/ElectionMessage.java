package fj.ac.usp.cs324.distrilab.common.model;

import java.io.Serializable;

public record ElectionMessage(
        String electionId,
        int initiatorId,
        int candidateId,
        int candidateJac
) implements Serializable {

    private static final long serialVersionUID = 1L;

    public ElectionMessage {
        if (electionId == null || electionId.isBlank()) {
            throw new IllegalArgumentException("electionId cannot be blank");
        }
        if (initiatorId < 0) {
            throw new IllegalArgumentException("initiatorId cannot be negative");
        }
        if (candidateId < 0) {
            throw new IllegalArgumentException("candidateId cannot be negative");
        }
        if (candidateJac < 0) {
            throw new IllegalArgumentException("candidateJac cannot be negative");
        }
    }
}