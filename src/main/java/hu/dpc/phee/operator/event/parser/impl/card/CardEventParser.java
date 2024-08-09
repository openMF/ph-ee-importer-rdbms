package hu.dpc.phee.operator.event.parser.impl.card;

import com.baasflow.commons.events.EventLogLevel;
import com.baasflow.commons.events.EventService;
import com.baasflow.commons.events.EventStatus;
import com.baasflow.commons.events.EventType;
import hu.dpc.phee.operator.config.model.Flow;
import hu.dpc.phee.operator.entity.card.BusinessProcessStatus;
import hu.dpc.phee.operator.entity.card.CardTransaction;
import hu.dpc.phee.operator.entity.card.CardTransactionRepository;
import hu.dpc.phee.operator.entity.task.Task;
import hu.dpc.phee.operator.entity.task.TaskRepository;
import hu.dpc.phee.operator.entity.tenant.ThreadLocalContextUtil;
import hu.dpc.phee.operator.entity.variable.Variable;
import hu.dpc.phee.operator.entity.variable.VariableRepository;
import hu.dpc.phee.operator.event.parser.EventParser;
import hu.dpc.phee.operator.event.parser.impl.EventRecord;
import hu.dpc.phee.operator.event.parser.impl.card.config.CardTransactionTransformerConfig;
import hu.dpc.phee.operator.tenants.TenantsService;
import hu.dpc.phee.operator.value.transformer.ValueTransformers;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Component
@Slf4j
public class CardEventParser implements EventParser {

    @Autowired
    private VariableRepository variableRepository;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private CardTransactionRepository cardTransactionRepository;

    @Autowired
    private CardTransactionTransformerConfig cardTransactionTransformerConfig;

    @Autowired
    private TenantsService tenantsService;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    private EventService eventService;

    @Autowired
    private ValueTransformers valueTransformers;

    @Override
    public boolean isAbleToProcess(List<EventRecord> eventRecords) {
        return eventRecords.stream()
                .anyMatch(e -> cardTransactionTransformerConfig.findFlow(e.getBpmnProcessId()).isPresent());
    }

    @Override
    public void process(List<EventRecord> eventRecords) {
        log.debug("processing {} records in CardEventParser", eventRecords.size());
        try {
            EventRecord first = eventRecords.get(0);
            String tenantName = first.getTenant();
            Long processInstanceKey = first.getProcessInstanceKey();
            if (processInstanceKey == null) {
                log.warn("no processInstanceKey found in record {}", first.jsonString());
                return;
            }
            String bpmn = first.getBpmnProcessId();
            if (StringUtils.isBlank(bpmn)) {
                log.warn("no bpmn found in record {}", first.jsonString());
                return;
            }
            Optional<Flow> flow = cardTransactionTransformerConfig.findFlow(bpmn);
            if (flow.isEmpty()) {
                throw new RuntimeException("no flow found for bpmn " + bpmn);
            }
            String direction = flow.get().getDirection();
            ThreadLocalContextUtil.setTenant(tenantsService.getTenantDataSource(tenantName));
            MDC.put("transactionId", String.valueOf(processInstanceKey));
            processEventRecords(eventRecords, bpmn, processInstanceKey, tenantName, direction);
        } catch (Exception e) {
            throw new RuntimeException("failed to process records", e);
        } finally {
            MDC.clear();
            ThreadLocalContextUtil.clear();
        }
    }

    private void processEventRecords(List<EventRecord> eventRecords, String bpmn, Long processInstanceKey, String tenantName, String direction) {
        log.info("processing CardTransaction event records for bpmn {} and processInstanceKey {} in tenant {}", bpmn, processInstanceKey, tenantName);
        transactionTemplate.executeWithoutResult(status -> {
            CardTransaction cardTransaction = retrieveOrCreateCardTransaction(processInstanceKey, direction);
            for (EventRecord eventRecord : eventRecords) {
                log.trace("CardEventParser processing record: {}", eventRecord.jsonString());
                try {
                    processEventRecord(cardTransaction, eventRecord);
                } catch (Exception e) {
                    log.error("failed to process record with CardEventParser: {}", eventRecord, e);
                }
            }
            cardTransactionRepository.save(cardTransaction);
        });
    }

    private CardTransaction retrieveOrCreateCardTransaction(long processInstanceKey, String direction) {
        CardTransaction cardTransaction = cardTransactionRepository.findByWorkflowInstanceKey(Long.toString(processInstanceKey));
        if (cardTransaction != null) {
            log.debug("found existing CardTransaction with id {}", processInstanceKey);
            return cardTransaction;
        }
        log.debug("creating new CardTransaction with id {}", processInstanceKey);
        CardTransaction newCardTransaction = new CardTransaction();
        newCardTransaction.setWorkflowInstanceKey(Long.toString(processInstanceKey));
        cardTransactionRepository.save(newCardTransaction);
        return newCardTransaction;
    }

    private void processEventRecord(CardTransaction cardTransaction, EventRecord eventRecord) {
        if (log.isTraceEnabled()) {
            log.trace("{} event is: {}", eventRecord.getValueType(), eventRecord.jsonString());
        } else {
            log.debug("event type is {}", eventRecord.getValueType());
        }
        switch (eventRecord.getValueType()) {
            case "PROCESS_INSTANCE" -> processInstance(cardTransaction, eventRecord);
            case "JOB" -> processJob(eventRecord);
            case "VARIABLE" -> processVariable(cardTransaction, eventRecord);
            case "INCIDENT" -> processIncident(cardTransaction, eventRecord);
            default -> log.error("unknown event type: {}", eventRecord.getValueType());
        }
    }

    private void processInstance(CardTransaction cardTransaction, EventRecord eventRecord) {
        String recordType = eventRecord.readProperty("$.recordType");
        String intent = eventRecord.readProperty("$.intent");

        if ("EVENT".equals(recordType) && "START_EVENT".equals(eventRecord.getBpmnElementType()) && "ELEMENT_ACTIVATED".equals(intent)) {
            cardTransaction.setStartedAt(new Date(eventRecord.getTimestamp()));
            cardTransaction.setLastUpdated(new Date(eventRecord.getTimestamp()));
            valueTransformers.applyForConstants(eventRecord.getBpmnProcessId(), cardTransaction);
            return;
        }

        if ("EVENT".equals(recordType) && "END_EVENT".equals(eventRecord.getBpmnElementType()) && "ELEMENT_COMPLETED".equals(intent)) {
            log.info("finishing transfer for processInstanceKey: {} at elementId: {}", eventRecord.getProcessInstanceKey(), eventRecord.getElementId());
            cardTransaction.setCompletedAt(new Date(eventRecord.getTimestamp()));
            if (StringUtils.isNotEmpty(eventRecord.getElementId()) && eventRecord.getElementId().contains("Failed")) {
                cardTransaction.setBusinessProcessStatus(BusinessProcessStatus.FAILED);
            } else {
                cardTransaction.setBusinessProcessStatus(BusinessProcessStatus.COMPLETED);
            }
            cardTransaction.setLastUpdated(new Date(eventRecord.getTimestamp()));
            return;
        }

        if ("EVENT".equals(recordType) && "EXCLUSIVE_GATEWAY".equals(eventRecord.getBpmnElementType()) && "ELEMENT_COMPLETED".equals(intent)) {
            log.info("exclusive gateway completed for processInstanceKey: {} at elementId: {}", eventRecord.getProcessInstanceKey(), eventRecord.getElementId());
            Task task = new Task()
                    .withWorkflowInstanceKey(eventRecord.getProcessInstanceKey())
                    .withWorkflowKey(eventRecord.getProcessDefinitionKey())
                    .withTimestamp(eventRecord.getTimestamp())
                    .withIntent(intent)
                    .withRecordType(recordType)
                    .withType("EXCLUSIVE_GATEWAY")
                    .withElementId(eventRecord.getElementId());
            taskRepository.save(task);
            return;
        }

        if ("EVENT".equals(recordType) && "TIMER".equals(eventRecord.getBpmnEventType()) && "ELEMENT_ACTIVATED".equals(intent)) {
            log.info("timer event for processInstanceKey: {} at elementId: {}", eventRecord.getProcessInstanceKey(), eventRecord.getElementId());
            Task task = new Task()
                    .withWorkflowInstanceKey(eventRecord.getProcessInstanceKey())
                    .withWorkflowKey(eventRecord.getProcessDefinitionKey())
                    .withTimestamp(eventRecord.getTimestamp())
                    .withIntent(intent)
                    .withRecordType(recordType)
                    .withType("TIMER")
                    .withElementId(eventRecord.getElementId());
            taskRepository.save(task);
            return;
        }

        if ("EVENT".equals(recordType) && "MESSAGE".equals(eventRecord.getBpmnEventType()) && "ELEMENT_ACTIVATED".equals(intent)) {
            log.info("message event for processInstanceKey: {} at elementId: {}", eventRecord.getProcessInstanceKey(), eventRecord.getElementId());
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

    private void processVariable(CardTransaction cardTransaction, EventRecord eventRecord) {
        log.debug("processing variable in flow {}", eventRecord.getBpmnProcessId());
        String variableName = eventRecord.readProperty("$.value.name");
        String variableValue = eventRecord.readProperty("$.value.value");
        String value = variableValue.startsWith("\"") && variableValue.endsWith("\"") ? StringEscapeUtils.unescapeJson(variableValue.substring(1, variableValue.length() - 1)) : variableValue;
        log.trace("{} = {}", variableName, variableValue);
        if (valueTransformers.applyForVariable(eventRecord.getBpmnProcessId(), variableName, cardTransaction, value)) {
            cardTransaction.setLastUpdated(new Date(eventRecord.getTimestamp()));
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

    private void processIncident(CardTransaction cardTransaction, EventRecord eventRecord) {
        log.warn("processing incident in flow {}", eventRecord.getBpmnProcessId());

        cardTransaction.setBusinessProcessStatus(BusinessProcessStatus.EXCEPTION);
        cardTransaction.setCompletedAt(new Date(eventRecord.getTimestamp()));
        cardTransaction.setLastUpdated(new Date(eventRecord.getTimestamp()));

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
        log.debug("processing job/task in flow {}", eventRecord.getBpmnProcessId());
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