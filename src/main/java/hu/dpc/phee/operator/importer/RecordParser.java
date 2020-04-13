package hu.dpc.phee.operator.importer;

import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operator.audit.BusinessKey;
import hu.dpc.phee.operator.audit.Task;
import hu.dpc.phee.operator.audit.TaskRepository;
import hu.dpc.phee.operator.audit.BusinessKeyRepository;
import hu.dpc.phee.operator.audit.Variable;
import hu.dpc.phee.operator.audit.VariableRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
public class RecordParser {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private static List<String> BUSINESS_ID_FIELDS = Arrays.asList("transactionId", "partyId");

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private VariableRepository variableRepository;

    @Autowired
    private BusinessKeyRepository businessKeyRepository;


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

        if (BUSINESS_ID_FIELDS.contains(name)) {
            BusinessKey businessKey = new BusinessKey();
            businessKey.setTimestamp(timestamp);
            businessKey.setWorkflowInstanceKey(workflowInstanceKey);
            businessKey.setBusinessKey(value);
            businessKey.setBusinessKeyType(name);
            businessKeyRepository.save(businessKey);
        }
    }

}
