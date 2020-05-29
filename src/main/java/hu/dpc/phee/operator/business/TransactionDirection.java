package hu.dpc.phee.operator.business;

import java.util.Arrays;

public enum TransactionDirection {
    INCOMING("PAYEE"),
    OUTGOING("PAYER"),
    IGNORED(null);

    private String initiator;

    TransactionDirection(String initiator) {
        this.initiator = initiator;
    }

    public static TransactionDirection fromInitiator(String initiator) {
        return Arrays.stream(values()).filter(it -> it.initiator.equals(initiator)).findFirst().orElse(IGNORED);
    }
}
