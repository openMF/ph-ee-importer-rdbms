package hu.dpc.phee.operator.event.parser.impl.transfer;

import com.baasflow.commons.events.EventLogLevel;
import com.baasflow.commons.events.EventService;
import com.baasflow.commons.events.EventStatus;
import com.baasflow.commons.events.EventType;
import hu.dpc.phee.operator.config.model.Flow;
import hu.dpc.phee.operator.entity.task.Task;
import hu.dpc.phee.operator.entity.task.TaskRepository;
import hu.dpc.phee.operator.entity.tenant.ThreadLocalContextUtil;
import hu.dpc.phee.operator.entity.transfer.Transfer;
import hu.dpc.phee.operator.entity.transfer.TransferRepository;
import hu.dpc.phee.operator.entity.transfer.TransferStatus;
import hu.dpc.phee.operator.entity.variable.Variable;
import hu.dpc.phee.operator.entity.variable.VariableRepository;
import hu.dpc.phee.operator.event.parser.EventParser;
import hu.dpc.phee.operator.event.parser.impl.EventRecord;
import hu.dpc.phee.operator.event.parser.impl.transfer.config.TransferTransformerConfig;
import hu.dpc.phee.operator.tenants.TenantsService;
import hu.dpc.phee.operator.value.transformer.ValueTransformers;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Component
public class TransferEventParser implements EventParser {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private VariableRepository variableRepository;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private TransferRepository transferRepository;

    @Autowired
    private TenantsService tenantsService;

    @Autowired
    private TransferTransformerConfig transferTransformerConfig;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    private EventService eventService;

    @Autowired
    private ValueTransformers valueTransformers;

    @Override
    public boolean isAbleToProcess(List<EventRecord> eventRecords) {
        return eventRecords.stream()
                .anyMatch(e -> transferTransformerConfig.findFlow(e.getBpmnProcessId()).isPresent());
    }

    @Override
    public void process(List<EventRecord> eventRecords) {
        logger.debug("processing {} records in TranferEventParser", eventRecords.size());
        try {
            EventRecord first = eventRecords.get(0);
            String tenantName = first.getTenant();
            Long processInstanceKey = first.getProcessInstanceKey();
            String bpmn = first.getBpmnProcessId();
            ThreadLocalContextUtil.setTenant(tenantsService.getTenantDataSource(tenantName));
            MDC.put("transactionId", String.valueOf(processInstanceKey));
            processEventRecords(eventRecords, bpmn, processInstanceKey, tenantName);
        } catch (Exception e) {
            logger.error("failed to process records", e);
        } finally {
            MDC.clear();
            ThreadLocalContextUtil.clear();
        }
    }

    private void processEventRecords(List<EventRecord> eventRecords, String bpmn, Long processInstanceKey, String tenantName) {
        logger.info("processing Transfer event records for bpmn {} and processInstanceKey {} in tenant {}", bpmn, processInstanceKey, tenantName);
        transactionTemplate.executeWithoutResult(status -> {
            Transfer transfer = retrieveOrCreateTransfer(bpmn, eventRecords.get(0));
            for (EventRecord eventRecord : eventRecords) {
                logger.trace("FileTransportEventParser processing record: {}", eventRecord.jsonString());
                try {
                    processEventRecord(transfer, eventRecord);
                } catch (Exception e) {
                    logger.error("failed to process record with TransferEventParser: {}", eventRecord, e);
                }
            }
            transferRepository.save(transfer);
        });
    }

    private void processEventRecord(Transfer transfer, EventRecord eventRecord) {
        if (logger.isTraceEnabled()) {
            logger.trace("{} event is: {}", eventRecord.getValueType(), eventRecord.jsonString());
        } else {
            logger.debug("event type is {}", eventRecord.getValueType());
        }
        switch (eventRecord.getValueType()) {
            case "PROCESS_INSTANCE" -> processInstance(transfer, eventRecord);
            case "JOB" -> processJob(eventRecord);
            case "VARIABLE" -> processVariable(transfer, eventRecord);
            case "INCIDENT" -> processIncident(transfer, eventRecord);
            default -> logger.error("unknown event type: {}", eventRecord.getValueType());
        }
    }

    private Transfer retrieveOrCreateTransfer(String bpmn, EventRecord eventRecord) {
        Transfer transfer = transferRepository.findByWorkflowInstanceKey(eventRecord.getProcessInstanceKey());
        if (transfer == null) {
            logger.debug("creating new Transfer for processInstanceKey: {}", eventRecord.getProcessInstanceKey());
            transfer = new Transfer(eventRecord.getProcessInstanceKey());
            transfer.setStatus(TransferStatus.IN_PROGRESS);
            transfer.setLastUpdated(eventRecord.getTimestamp());
            Optional<Flow> config = transferTransformerConfig.findFlow(bpmn);
            if (config.isPresent()) {
                transfer.setDirection(config.get().getDirection());
            } else {
                logger.error("No config found for bpmn: {}", bpmn);
            }
            transferRepository.save(transfer);
        } else {
            logger.debug("found existing Transfer for processInstanceKey: {}", eventRecord.getProcessInstanceKey());
        }
        return transfer;
    }

    private void processInstance(Transfer transfer, EventRecord eventRecord) {
        String recordType = eventRecord.readProperty("$.recordType");
        String intent = eventRecord.readProperty("$.intent");

        if ("EVENT".equals(recordType) && "START_EVENT".equals(eventRecord.getBpmnElementType()) && "ELEMENT_ACTIVATED".equals(intent)) {
            transfer.setStartedAt(new Date(eventRecord.getTimestamp()));
            transfer.setLastUpdated(eventRecord.getTimestamp());
            valueTransformers.applyForConstants(eventRecord.getBpmnProcessId(), transfer);
        }

        if ("EVENT".equals(recordType) && "END_EVENT".equals(eventRecord.getBpmnElementType()) && "ELEMENT_COMPLETED".equals(intent)) {
            logger.info("finishing transfer for processInstanceKey: {} at elementId: {}", eventRecord.getProcessInstanceKey(), eventRecord.getElementId());
            transfer.setCompletedAt(new Date(eventRecord.getTimestamp()));
            if (StringUtils.isNotEmpty(eventRecord.getElementId()) && eventRecord.getElementId().contains("Failed")) {
                transfer.setStatus(TransferStatus.FAILED);
            } else {
                transfer.setStatus(TransferStatus.COMPLETED);
            }
            transfer.setLastUpdated(eventRecord.getTimestamp());
        }

        if ("EVENT".equals(recordType) && "EXCLUSIVE_GATEWAY".equals(eventRecord.getBpmnElementType()) && "ELEMENT_COMPLETED".equals(intent)) {
            logger.info("exclusive gateway completed for processInstanceKey: {} at elementId: {}", eventRecord.getProcessInstanceKey(), eventRecord.getElementId());
            Task task = new Task()
                    .withWorkflowInstanceKey(eventRecord.getProcessInstanceKey())
                    .withWorkflowKey(eventRecord.getProcessDefinitionKey())
                    .withTimestamp(eventRecord.getTimestamp())
                    .withIntent(intent)
                    .withRecordType(recordType)
                    .withType("EXCLUSIVE_GATEWAY")
                    .withElementId(eventRecord.getElementId());
            taskRepository.save(task);
        }

        if ("EVENT".equals(recordType) && "TIMER".equals(eventRecord.getBpmnEventType()) && "ELEMENT_ACTIVATED".equals(intent)) {
            logger.info("timer event for processInstanceKey: {} at elementId: {}", eventRecord.getProcessInstanceKey(), eventRecord.getElementId());
            Task task = new Task()
                    .withWorkflowInstanceKey(eventRecord.getProcessInstanceKey())
                    .withWorkflowKey(eventRecord.getProcessDefinitionKey())
                    .withTimestamp(eventRecord.getTimestamp())
                    .withIntent(intent)
                    .withRecordType(recordType)
                    .withType("TIMER")
                    .withElementId(eventRecord.getElementId());
            taskRepository.save(task);
        }

        if ("EVENT".equals(recordType) && "MESSAGE".equals(eventRecord.getBpmnEventType()) && "ELEMENT_ACTIVATED".equals(intent)) {
            logger.info("message event for processInstanceKey: {} at elementId: {}", eventRecord.getProcessInstanceKey(), eventRecord.getElementId());
            Task task = new Task()
                    .withWorkflowInstanceKey(eventRecord.getProcessInstanceKey())
                    .withWorkflowKey(eventRecord.getProcessDefinitionKey())
                    .withTimestamp(eventRecord.getTimestamp())
                    .withIntent(intent)
                    .withRecordType(recordType)
                    .withType("MESSAGE")
                    .withElementId(eventRecord.getElementId());
            taskRepository.save(task);
        }
    }

    private void processVariable(Transfer transfer, EventRecord eventRecord) {
        logger.debug("processing variable in flow {}", eventRecord.getBpmnProcessId());
        String variableName = eventRecord.readProperty("$.value.name");
        String variableValue = eventRecord.readProperty("$.value.value");
        String value = variableValue.startsWith("\"") && variableValue.endsWith("\"") ? StringEscapeUtils.unescapeJson(variableValue.substring(1, variableValue.length() - 1)) : variableValue;
        logger.trace("{} = {}", variableName, variableValue);
        if (valueTransformers.applyForVariable(eventRecord.getBpmnProcessId(), variableName, transfer, value)) {
            transfer.setLastUpdated(eventRecord.getTimestamp());
        }
        Variable variable = new Variable()
                .withWorkflowInstanceKey(eventRecord.getProcessInstanceKey())
                .withName(variableName)
                .withWorkflowKey(eventRecord.getProcessDefinitionKey())
                .withTimestamp(eventRecord.getTimestamp())
                .withPosition(eventRecord.getId())
                .withValue(variableValue);
        variableRepository.save(variable);
    }

    private void processIncident(Transfer transfer, EventRecord eventRecord) {
        logger.warn("processing incident in flow {}", eventRecord.getBpmnProcessId());

        transfer.setStatus(TransferStatus.EXCEPTION);
        transfer.setCompletedAt(new Date(eventRecord.getTimestamp()));
        transfer.setLastUpdated(eventRecord.getTimestamp());

        eventService.sendEvent(event -> event
                .setSourceModule("importer")
                .setEventLogLevel(EventLogLevel.ERROR)
                .setEventType(EventType.audit)
                .setEvent("Incident event received for flow")
                .setEventStatus(EventStatus.failure)
                .setTenantId(eventRecord.getTenant())
                .setCorrelationIds(Map.of(
                        "processInstanceId", eventRecord.getProcessInstanceKey().toString()
                ))
                .setPayload(eventRecord.jsonString())
                .setPayloadType("string")
        );

        Variable variable = new Variable()
                .withWorkflowInstanceKey(eventRecord.getProcessInstanceKey())
                .withName("exception")
                .withWorkflowKey(eventRecord.getProcessDefinitionKey())
                .withTimestamp(eventRecord.getTimestamp())
                .withPosition(eventRecord.getId())
                .withValue(StringEscapeUtils.escapeJson(eventRecord.jsonString()));
        variableRepository.save(variable);
    }

    private void processJob(@NotNull EventRecord eventRecord) {
        logger.debug("processing job/task in flow {}", eventRecord.getBpmnProcessId());
        Task task = new Task()
                .withWorkflowInstanceKey(eventRecord.getProcessInstanceKey())
                .withWorkflowKey(eventRecord.getProcessDefinitionKey())
                .withTimestamp(eventRecord.getTimestamp())
                .withIntent(eventRecord.readProperty("$.intent"))
                .withRecordType(eventRecord.getValueType())
                .withType(eventRecord.readProperty("$.value.type"))
                .withElementId(eventRecord.getElementId());
        taskRepository.save(task);
    }

    @Override
    public @NotNull String getBeanName() {
        return this.getClass().getName();
    }
}