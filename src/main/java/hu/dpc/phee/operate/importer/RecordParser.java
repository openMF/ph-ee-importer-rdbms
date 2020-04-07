package hu.dpc.phee.operate.importer;

import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operate.importer.persistence.Task;
import hu.dpc.phee.operate.importer.persistence.TaskRepository;
import hu.dpc.phee.operate.importer.persistence.Transaction;
import hu.dpc.phee.operate.importer.persistence.TransactionRepository;
import hu.dpc.phee.operate.importer.persistence.Variable;
import hu.dpc.phee.operate.importer.persistence.VariableRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
public class RecordParser {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private VariableRepository variableRepository;

    @Autowired
    private TransactionRepository transactionRepository;


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
        variable.setWorkflowKey(json.read("$.value.workflowKey"));
        variable.setTimestamp(timestamp);
        String name = json.read("$.value.name");
        variable.setName(name);
        String value = json.read("$.value.value");
        variable.setValue(value);
        variableRepository.save(variable);

        List<String> businessIds = Arrays.asList("transactionId", "partyId");
        if (businessIds.contains(name)) {
            Transaction transaction = new Transaction();
            transaction.setTimestamp(timestamp);
            transaction.setWorkflowInstanceKey(workflowInstanceKey);
            transaction.setBusinessKey(value);
            transaction.setBusinessKeyType(name);
            transactionRepository.save(transaction);
        }
    }

}
