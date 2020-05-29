package hu.dpc.phee.operator.business;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public enum TransactionDirection {
    INCOMING(Arrays.asList("PayeeQuoteTransfer", "PayerTransactionRequest")),
    OUTGOING(Arrays.asList("PayerFundTransfer", "PayeeTransactionRequest")),
    IGNORED(Collections.emptyList());

    private List<String> processes;

    TransactionDirection(List<String> processes) {
        this.processes = processes;
    }

    public List<String> getProcesses() {
        return processes;
    }

    public static TransactionDirection fromBpmn(String processId) {
        if(processId != null) {
            String process = processId.split("-")[0];
            return Arrays.stream(TransactionDirection.values())
                    .filter(d -> d.getProcesses().contains(process))
                    .findFirst().orElse(IGNORED);
        } else {
            return IGNORED;
        }
    }
}
