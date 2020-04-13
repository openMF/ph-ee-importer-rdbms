package hu.dpc.phee.operator.business;

import com.jayway.jsonpath.DocumentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

@Component
public class TransactionParser {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private static Map<String, Consumer<Pair<Transaction, String>>> BUSINESS_FIELDS = new HashMap<>();

    static {
        BUSINESS_FIELDS.put("transactionId", pair -> pair.getFirst().setTransactionId(pair.getSecond()));
        BUSINESS_FIELDS.put("partyId", pair -> pair.getFirst().setTransactionId(pair.getSecond()));
    }
    
    @Autowired
    private TransactionRepository transactionRepository;

    private Map<Long, Transaction> inflightTransactions = new HashMap<>();


    public void parseVariable(DocumentContext json) {
        String name = json.read("$.value.name");

        if (BUSINESS_FIELDS.keySet().contains(name)) {
            Long workflowInstanceKey = json.read("$.value.workflowInstanceKey");
            String value = json.read("$.value.value");

            Transaction transaction = getOrCreateTransaction(workflowInstanceKey);
            BUSINESS_FIELDS.get(name).accept(Pair.of(transaction, value));
        }
    }

    public void parseWorkflowElement(DocumentContext json) {
        String bpmnElementType = json.read("$.value.bpmnElementType");
        String intent = json.read("$.intent");

        if ("START_EVENT".equals(bpmnElementType) && "ELEMENT_ACTIVATED".equals(intent)) {
            Long workflowInstanceKey = json.read("$.value.workflowInstanceKey");
            Transaction transaction = getOrCreateTransaction(workflowInstanceKey);
            inflightTransactions.put(workflowInstanceKey, transaction);
            logger.debug("started in-flight transaction {}", transaction.getWorkflowInstanceKey());
        }

        if ("END_EVENT".equals(bpmnElementType) && "ELEMENT_ACTIVATED".equals(intent)) {
            Long workflowInstanceKey = json.read("$.value.workflowInstanceKey");
            Transaction transaction = inflightTransactions.remove(workflowInstanceKey);
            if (transaction == null) {
                logger.error("failed to find in-flight transaction {}", workflowInstanceKey);
            } else {
                transactionRepository.save(transaction);
                logger.debug("saved finished transaction {}", transaction.getWorkflowInstanceKey());
            }
        }
    }

    private synchronized Transaction getOrCreateTransaction(Long workflowInstanceKey) {
        Transaction transaction = inflightTransactions.get(workflowInstanceKey);
        if (transaction == null) {
            transaction = new Transaction(workflowInstanceKey);
            inflightTransactions.put(workflowInstanceKey, transaction);
        }
        return transaction;
    }
}
