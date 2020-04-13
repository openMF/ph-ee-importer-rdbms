package hu.dpc.phee.operator.importer;

import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operator.audit.BusinessKey;
import hu.dpc.phee.operator.audit.BusinessKeyRepository;
import hu.dpc.phee.operator.audit.Task;
import hu.dpc.phee.operator.audit.TaskRepository;
import hu.dpc.phee.operator.audit.Variable;
import hu.dpc.phee.operator.audit.VariableRepository;
import hu.dpc.phee.operator.business.Transaction;
import hu.dpc.phee.operator.business.TransactionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

@Component
public class RecordParser {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private static Map<String, Consumer<Pair<Transaction, String>>> BUSINESS_FIELDS = new HashMap<>();

    static {
        BUSINESS_FIELDS.put("transactionId", pair -> pair.getFirst().setTransactionId(pair.getSecond()));
        BUSINESS_FIELDS.put("partyId", pair -> pair.getFirst().setTransactionId(pair.getSecond()));
    }

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private VariableRepository variableRepository;

    @Autowired
    private BusinessKeyRepository businessKeyRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    private Map<Long, Transaction> inflightTransactions = new HashMap<>();


    public void parseTask(DocumentContext json) {
        String type = json.read("$.value.type");
        if (type == null) {
            return;
        }

        String intent = json.read("$.intent");
        String recordType = json.read("$.recordType");
        logger.info("{} {} {}", intent, recordType, type);

        Task task = new Task();
        task.setWorkflowInstanceKey(json.read("$.value.workflowInstanceKey"));
        task.setWorkflowKey(json.read("$.value.workflowKey"));
        task.setTimestamp(json.read("$.timestamp"));
        task.setIntent(intent);
        task.setRecordType(recordType);
        task.setType(type);
        taskRepository.save(task);
    }

    public void parseVariable(DocumentContext json) {
        Long workflowInstanceKey = json.read("$.value.workflowInstanceKey");
        Long timestamp = json.read("$.timestamp");

        Variable variable = new Variable();
        variable.setWorkflowInstanceKey(workflowInstanceKey);
        variable.setTimestamp(timestamp);
        variable.setWorkflowKey(json.read("$.value.workflowKey"));
        String name = json.read("$.value.name");
        variable.setName(name);
        String value = json.read("$.value.value");
        variable.setValue(value);
        variableRepository.save(variable);

        if (BUSINESS_FIELDS.keySet().contains(name)) {
            BusinessKey businessKey = new BusinessKey();
            businessKey.setTimestamp(timestamp);
            businessKey.setWorkflowInstanceKey(workflowInstanceKey);
            businessKey.setBusinessKeyType(name);
            businessKey.setBusinessKey(value);
            businessKeyRepository.save(businessKey);

            // enrich transaction
            Transaction transaction = getOrCreateTransaction(workflowInstanceKey);

            if (transaction != null) {
                BUSINESS_FIELDS.get(name).accept(Pair.of(transaction, value));
                transactionRepository.save(transaction);
            } else {
                logger.error("failed to find transaction for workflowInstanceKey {}", workflowInstanceKey);
                logger.error("{} in flight transactions", inflightTransactions.size());
            }
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
