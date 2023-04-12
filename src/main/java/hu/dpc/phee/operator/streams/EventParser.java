package hu.dpc.phee.operator.streams;

import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operator.entity.task.Task;
import hu.dpc.phee.operator.entity.task.TaskRepository;
import hu.dpc.phee.operator.entity.transfer.Transfer;
import hu.dpc.phee.operator.entity.transfer.TransferRepository;
import hu.dpc.phee.operator.entity.variable.Variable;
import hu.dpc.phee.operator.entity.variable.VariableRepository;
import hu.dpc.phee.operator.importer.JsonPathReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class EventParser {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    VariableRepository variableRepository;

    @Autowired
    TaskRepository taskRepository;

    @Autowired
    TransferRepository transferRepository;


    public String retrieveTenant(DocumentContext record) {
        String bpmnProcessIdWithTenant = record.read("$.value.bpmnProcessId", String.class);
        String[] split = bpmnProcessIdWithTenant.split("-");
        if (split.length < 2) {
            throw new RuntimeException("Invalid bpmnProcessId, has no tenant information: '" + bpmnProcessIdWithTenant + "'");
        }
        return split[1];
    }

    public Transfer retrieveOrCreateTransfer(DocumentContext record) {
        Long processInstanceKey = record.read("$.value.processInstanceKey", Long.class);

        Transfer transfer = transferRepository.findByWorkflowInstanceKey(processInstanceKey);
        if (transfer == null) {
            logger.debug("creating new Transfer for processInstanceKey: {}", processInstanceKey);
            transfer = new Transfer(processInstanceKey);
            transferRepository.save(transfer);
        } else {
            logger.debug("found existing Transfer for processInstanceKey: {}", processInstanceKey);
        }
        return transfer;
    }

    public void process(String tenantName, Transfer transfer, String rawData) {
        DocumentContext record = JsonPathReader.parse(rawData);
        logger.info("from kafka: {}", record.jsonString());

        String recordType = record.read("$.valueType", String.class);
        logger.debug("processing {} event", recordType);

        Long workflowKey = record.read("$.value.processDefinitionKey");
        Long workflowInstanceKey = record.read("$.value.processInstanceKey");
        Long timestamp = record.read("$.timestamp");

        List<Object> entities = switch (recordType) {
            case "DEPLOYMENT", "VARIABLE_DOCUMENT", "WORKFLOW_INSTANCE", "PROCESS_INSTANCE" -> List.of();

            case "JOB" -> List.of(
                    new Task()
                            .withWorkflowInstanceKey(workflowInstanceKey)
                            .withWorkflowKey(workflowKey)
                            .withTimestamp(timestamp)
                            .withIntent(record.read("$.intent", String.class))
                            .withRecordType(recordType)
                            .withType(record.read("$.value.type", String.class))
                            .withElementId(record.read("$.value.elementId", String.class))
            );

            case "VARIABLE" -> List.of(
                    new Variable()
                            .withWorkflowInstanceKey(workflowInstanceKey)
                            .withName(record.read("$.value.name", String.class))
                            .withWorkflowKey(workflowKey)
                            .withTimestamp(timestamp)
                            .withValue(record.read("$.value.value", String.class)));

            case "INCIDENT" -> {
                logger.warn("TODO: not processing INCIDENT record for now");
                yield List.of();
            }

            default -> throw new IllegalStateException("Unexpected event type: " + recordType);
        };

        if (entities.size() != 0) {
            logger.info("Saving {} entities", entities.size());
            entities.forEach(entity -> {
                switch (entity) {
                    case Variable variable -> variableRepository.save(variable);
                    case Task task -> taskRepository.save(task);
                    case Transfer transferEntity -> transferRepository.save(transferEntity);
                    default -> throw new IllegalStateException("Unexpected entity type: " + entity.getClass());
                }
            });
        }
    }
}
