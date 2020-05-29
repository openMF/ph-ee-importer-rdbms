package hu.dpc.phee.operator.business;

import java.util.Arrays;

public enum TransactionDirection {
    INCOMING("payee"),
    OUTGOING("payer"),
    IGNORED("ignored");

    private String prefix;

    TransactionDirection(String prefix) {
        this.prefix = prefix;
    }

    public static TransactionDirection fromBpmn(String processId) {
        if(processId != null) {
            return Arrays.stream(TransactionDirection.values())
                    .filter(d -> processId.toLowerCase().startsWith(d.prefix))
                    .findFirst()
                    .orElse(IGNORED);
        } else {
            return IGNORED;
        }
    }
}
