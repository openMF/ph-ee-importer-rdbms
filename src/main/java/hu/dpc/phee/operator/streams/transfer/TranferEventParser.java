package hu.dpc.phee.operator.streams.transfer;

import com.baasflow.commons.events.EventLogLevel;
import com.baasflow.commons.events.EventService;
import com.baasflow.commons.events.EventStatus;
import com.baasflow.commons.events.EventType;
import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operator.config.transformer.Flow;
import hu.dpc.phee.operator.config.transformer.Transformer;
import hu.dpc.phee.operator.entity.task.Task;
import hu.dpc.phee.operator.entity.task.TaskRepository;
import hu.dpc.phee.operator.entity.tenant.ThreadLocalContextUtil;
import hu.dpc.phee.operator.entity.transfer.Transfer;
import hu.dpc.phee.operator.entity.transfer.TransferRepository;
import hu.dpc.phee.operator.entity.transfer.TransferStatus;
import hu.dpc.phee.operator.entity.variable.Variable;
import hu.dpc.phee.operator.entity.variable.VariableRepository;
import hu.dpc.phee.operator.importer.JsonPathReader;
import hu.dpc.phee.operator.streams.EventParser;
import hu.dpc.phee.operator.streams.EventParserUtil;
import hu.dpc.phee.operator.streams.transfer.config.TransferTransformerConfig;
import hu.dpc.phee.operator.tenants.TenantsService;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.util.Pair;
import org.springframework.orm.jpa.JpaOptimisticLockingFailureException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.sql.DataSource;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathFactory;
import java.io.StringReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

@Component
public class TranferEventParser implements EventParser {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    VariableRepository variableRepository;

    @Autowired
    TaskRepository taskRepository;

    @Autowired
    TransferRepository transferRepository;

    @Autowired
    TenantsService tenantsService;

    @Autowired
    TransferTransformerConfig transferTransformerConfig;

        @Autowired
        private TransactionTemplate transactionTemplate;

    @Autowired
    EventService eventService;

    private DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
    private XPathFactory xPathFactory = XPathFactory.newInstance();

    public Transfer retrieveOrCreateTransfer(String bpmn, DocumentContext record) {
        Long processInstanceKey = record.read("$.value.processInstanceKey", Long.class);
        Long timestamp = record.read("$.timestamp", Long.class);

        Transfer transfer = transferRepository.findByWorkflowInstanceKey(processInstanceKey);
        if (transfer == null) {
            logger.debug("creating new Transfer for processInstanceKey: {}", processInstanceKey);
            transfer = new Transfer(processInstanceKey);
            transfer.setStatus(TransferStatus.IN_PROGRESS);
            transfer.setLastUpdated(timestamp);
            Optional<Flow> config = transferTransformerConfig.findFlow(bpmn);
            if (config.isPresent()) {
                transfer.setDirection(config.get().getDirection());
            } else {
                logger.error("No config found for bpmn: {}", bpmn);
            }
            transferRepository.save(transfer);
        } else {
            logger.debug("found existing Transfer for processInstanceKey: {}", processInstanceKey);
        }
        return transfer;
    }

    public void process(String bpmn, String tenantName, Transfer transfer, DocumentContext record) {
        logger.info("from kafka: {}", record.jsonString());

        String valueType = record.read("$.valueType", String.class);
        logger.debug("processing {} event", valueType);

        Long workflowKey = record.read("$.value.processDefinitionKey");
        Long workflowInstanceKey = record.read("$.value.processInstanceKey");
        Long position = record.read("$.position", Long.class);
        Long timestamp = record.read("$.timestamp");
        String bpmnElementType = record.read("$.value.bpmnElementType");
        String bpmnEventType = record.read("$.value.bpmnEventType");
        String elementId = record.read("$.value.elementId");

        List<Object> entities = switch (valueType) {
            case "DEPLOYMENT", "VARIABLE_DOCUMENT", "WORKFLOW_INSTANCE" -> List.of();
            case "PROCESS_INSTANCE" -> {
                String recordType = record.read("$.recordType", String.class);
                String intent = record.read("$.intent", String.class);
                if ("EVENT".equals(recordType) && "START_EVENT".equals(bpmnElementType) && "ELEMENT_ACTIVATED".equals(intent)) {
                    transfer.setStartedAt(new Date(timestamp));
                    transfer.setLastUpdated(timestamp);

                    List<Transformer> constantTransformers = transferTransformerConfig.getFlows().stream()
                            .filter(it -> bpmn.equalsIgnoreCase(it.getName()))
                            .flatMap(it -> it.getTransformers().stream())
                            .filter(it -> Strings.isNotBlank(it.getConstant()))
                            .toList();

                    logger.debug("found {} constant transformers for flow start {}", constantTransformers.size(), bpmn);
                    constantTransformers.forEach(it -> applyTransformer(timestamp, transfer, null, null, it));
                    yield List.of();
                }

                if ("EVENT".equals(recordType) && "END_EVENT".equals(bpmnElementType) && "ELEMENT_COMPLETED".equals(intent)) {
                    logger.info("finishing transfer for processInstanceKey: {} at elementId: {}", workflowInstanceKey, elementId);
                    transfer.setCompletedAt(new Date(timestamp));
                    if (StringUtils.isNotEmpty(elementId) && elementId.contains("Failed")) {
                        transfer.setStatus(TransferStatus.FAILED);
                    } else {
                        transfer.setStatus(TransferStatus.COMPLETED);
                    }
                    transfer.setLastUpdated(timestamp);

                    yield List.of();
                }

                if ("EVENT".equals(recordType) && "EXCLUSIVE_GATEWAY".equals(bpmnElementType) && "ELEMENT_COMPLETED".equals(intent)) {
                    logger.info("exclusive gateway completed for processInstanceKey: {} at elementId: {}", workflowInstanceKey, elementId);
                    yield List.of(new Task()
                            .withWorkflowInstanceKey(workflowInstanceKey)
                            .withWorkflowKey(workflowKey)
                            .withTimestamp(timestamp)
                            .withIntent(intent)
                            .withRecordType(recordType)
                            .withType("EXCLUSIVE_GATEWAY")
                            .withElementId(elementId)
                    );
                }

                if ("EVENT".equals(recordType) && "TIMER".equals(bpmnEventType) && "ELEMENT_ACTIVATED".equals(intent)) {
                    logger.info("timer event for processInstanceKey: {} at elementId: {}", workflowInstanceKey, elementId);
                    yield List.of(new Task()
                            .withWorkflowInstanceKey(workflowInstanceKey)
                            .withWorkflowKey(workflowKey)
                            .withTimestamp(timestamp)
                            .withIntent(intent)
                            .withRecordType(recordType)
                            .withType("TIMER")
                            .withElementId(elementId)
                    );
                }

                if ("EVENT".equals(recordType) && "MESSAGE".equals(bpmnEventType) && "ELEMENT_ACTIVATED".equals(intent)) {
                    logger.info("message event for processInstanceKey: {} at elementId: {}", workflowInstanceKey, elementId);
                    yield List.of(new Task()
                            .withWorkflowInstanceKey(workflowInstanceKey)
                            .withWorkflowKey(workflowKey)
                            .withTimestamp(timestamp)
                            .withIntent(intent)
                            .withRecordType(recordType)
                            .withType("MESSAGE")
                            .withElementId(elementId)
                    );
                }

                yield List.of();
            }

            case "JOB" -> List.of(
                    new Task()
                            .withWorkflowInstanceKey(workflowInstanceKey)
                            .withWorkflowKey(workflowKey)
                            .withTimestamp(timestamp)
                            .withIntent(record.read("$.intent", String.class))
                            .withRecordType(valueType)
                            .withType(record.read("$.value.type", String.class))
                            .withElementId(elementId)
            );

            case "VARIABLE" -> {
                String variableName = record.read("$.value.name", String.class);
                String variableValue = record.read("$.value.value", String.class);
                String value = variableValue.startsWith("\"") && variableValue.endsWith("\"") ? StringEscapeUtils.unescapeJson(variableValue.substring(1, variableValue.length() - 1)) : variableValue;

                logger.debug("finding transformers for bpmn: {} and variable: {}", bpmn, variableName);
                List<Transformer> matchingTransformers = transferTransformerConfig.getFlows().stream()
                        .filter(it -> bpmn.equalsIgnoreCase(it.getName()))
                        .flatMap(it -> it.getTransformers().stream())
                        .filter(it -> variableName.equalsIgnoreCase(it.getVariableName()))
                        .toList();

                matchingTransformers.forEach(transformer -> applyTransformer(timestamp, transfer, variableName, value, transformer));

                yield List.of(
                        new Variable()
                                .withWorkflowInstanceKey(workflowInstanceKey)
                                .withName(variableName)
                                .withWorkflowKey(workflowKey)
                                .withTimestamp(timestamp)
                                .withPosition(position)
                                .withValue(value));
            }

            case "INCIDENT" -> {
                logger.warn("failing Transfer {} based on incident event", transfer.getTransactionId());
                transfer.setStatus(TransferStatus.EXCEPTION);
                transfer.setCompletedAt(new Date(timestamp));
                transfer.setLastUpdated(timestamp);

                sendIncidentAuditlog(tenantName, transfer, record.jsonString());
                yield List.of(
                        new Variable()
                                .withWorkflowInstanceKey(workflowInstanceKey)
                                .withName("exception")
                                .withWorkflowKey(workflowKey)
                                .withTimestamp(timestamp)
                                .withPosition(position)
                                .withValue(StringEscapeUtils.escapeJson(record.jsonString()))
                );
            }

            case "PROCESS_EVENT", "TIMER" -> {
                logger.warn("ignoring {} type event record", valueType);
                yield List.of();
            }

            default -> {
                logger.error("Unexpected event type: {}", valueType);
                yield List.of();
            }
        };

        if (!entities.isEmpty()) {
            logger.info("Saving {} entities", entities.size());
            entities.forEach(entity -> {
                if (entity instanceof Variable) {
                    try {
                        variableRepository.saveIfFresh((Variable) entity);
                    } catch (JpaOptimisticLockingFailureException e) {
                        logger.warn("ignoring OptimisticLockingFailureException when saving Variable");
                    }
                } else if (entity instanceof Task) {
                    taskRepository.save((Task) entity);
                } else {
                    throw new IllegalStateException("Unexpected entity type: " + entity.getClass());
                }
            });

            transferRepository.save(transfer);
        }
    }

    private void sendIncidentAuditlog(String tenantName, Transfer transfer, String rawData) {
        eventService.sendEvent(event -> event
                .setSourceModule("importer")
                .setEventLogLevel(EventLogLevel.ERROR)
                .setEventType(EventType.audit)
                .setEvent("Incident event received for flow")
                .setEventStatus(EventStatus.failure)
                .setTenantId(tenantName)
                .setCorrelationIds(Map.of(
                        "internalCorrelationId", transfer.getTransactionId(),
                        "processInstanceId", transfer.getWorkflowInstanceKey().toString()
                ))
                .setPayload(rawData)
                .setPayloadType("string")
        );
    }

    private void applyTransformer(Long timestamp, Transfer transfer, String variableName, String variableValue, Transformer transformer) {
        logger.debug("applying transformer for field: {}", transformer.getField());
        try {
            String fieldName = transformer.getField();
            String dateFormat = transformer.getDateFormat();
            if (Strings.isNotBlank(transformer.getConstant())) {
                logger.debug("setting constant value: {}", transformer.getConstant());
                setPropertyValue(timestamp, transfer, fieldName, transformer.getConstant(), dateFormat);
                return;
            }

            if (Strings.isNotBlank(transformer.getJsonPath())) {
                logger.debug("applying jsonpath for variable {}", variableName);
                DocumentContext json = JsonPathReader.parse(variableValue);
                Object result = json.read(transformer.getJsonPath());
                logger.debug("jsonpath result: {} for variable {}", result, variableName);

                String value = null;
                if (result != null) {
                    if (result instanceof List) {
                        value = ((List<?>) result).stream().map(Object::toString).collect(Collectors.joining(" "));
                    } else {
                        value = result.toString();
                    }
                    setPropertyValue(timestamp, transfer, fieldName, value, dateFormat);
                }

                if (StringUtils.isBlank(value)) {
                    logger.error("null result when setting field {} from variable {}. Jsonpath: {}, variable value: {}", fieldName, variableName, transformer.getJsonPath(), variableValue);
                }
                return;
            }

            if (Strings.isNotBlank(transformer.getXpath())) {
                logger.debug("applying xpath for variable {}", variableName);
                Document document = documentBuilderFactory.newDocumentBuilder().parse(new InputSource(new StringReader(variableValue)));
                String result = xPathFactory.newXPath().compile(transformer.getXpath()).evaluate(document);
                logger.debug("xpath result: {} for variable {}", result, variableName);
                if (StringUtils.isNotBlank(result)) {
                    setPropertyValue(timestamp, transfer, fieldName, result, dateFormat);
                } else {
                    logger.error("null result when setting field {} from variable {}. Xpath: {}, variable value: {}", fieldName, variableName, transformer.getXpath(), variableValue);
                }
                return;
            }

            logger.debug("setting simple variable value: {} for variable {}", variableValue, variableName);
            setPropertyValue(timestamp, transfer, fieldName, variableValue, dateFormat);

        } catch (Exception e) {
            logger.error("failed to apply transformer {} to variable {}", transformer, variableName, e);
        }
    }

    private void setPropertyValue(Long timestamp, Transfer transfer, String fieldName, String variableValue, String dateFormat) {
        if (Date.class.getName().equals(PropertyAccessorFactory.forBeanPropertyAccess(transfer).getPropertyType(fieldName).getName())) {
            try {
                logger.debug("Parsing date {} with format {}", variableValue, dateFormat);
                PropertyAccessorFactory.forBeanPropertyAccess(transfer).setPropertyValue(fieldName, new SimpleDateFormat(dateFormat).parse(variableValue));
            } catch (ParseException pe) {
                logger.warn("failed to parse date {} with format {}", variableValue, dateFormat);
            }
        } else {
            PropertyAccessorFactory.forBeanPropertyAccess(transfer).setPropertyValue(fieldName, variableValue);
        }
        transfer.setLastUpdated(timestamp);
    }

    @Override
    public boolean isAbleToProcess(Collection<DocumentContext> parsedRecords) {
        for (DocumentContext record : parsedRecords) {
            try {
                Pair<String, String> bpmnAndTenant = EventParserUtil.retrieveTenant(record);
                String bpmn = bpmnAndTenant.getFirst();
                if (transferTransformerConfig.findFlow(bpmn).isPresent()) {
                    return true;
                }
            } catch (Exception e) {
                logger.trace("could not resolve bpmn name from record: {}", record);
            }
        }
        return false;
    }

    @Override
    public void process(Collection<DocumentContext> parsedRecords) {
        try {
            String bpmn = null;
            String tenantName = null;
            DocumentContext sample = null;
            for (DocumentContext record : parsedRecords) {
                sample = record;
                try {
                    Pair<String, String> bpmnAndTenant = EventParserUtil.retrieveTenant(record);
                    bpmn = bpmnAndTenant.getFirst();
                    tenantName = bpmnAndTenant.getSecond();
                    DataSource tenant = tenantsService.getTenantDataSource(tenantName);
                    ThreadLocalContextUtil.setTenant(tenant);
                    break;
                } catch (Exception e) {
                    logger.trace("could not resolve bpmn name from record: {}", record);
                }
            }

            Validate.notNull(bpmn, "could not resolve bpmn name");
            Validate.notNull(tenantName, "could not resolve tenant name");
            Validate.notNull(sample, "could not resolve sample");

            final String _bpmn = bpmn;
            final String _tenantName = tenantName;
            final DocumentContext _sample = sample;
            transactionTemplate.executeWithoutResult(status -> {
                doProcessRecords(parsedRecords, _bpmn, _sample, _tenantName);
            });
        } catch (Exception e) {
            logger.error("failed to process batch", e);
        } finally {
            ThreadLocalContextUtil.clear();
        }
    }

    private void doProcessRecords(Collection<DocumentContext> parsedRecords, String bpmn, DocumentContext sample, String tenantName) {
        Transfer transfer = retrieveOrCreateTransfer(bpmn, sample);
        try {
            MDC.put("transactionId", transfer.getTransactionId());
            for (DocumentContext record : parsedRecords) {
                try {
                    process(bpmn, tenantName, transfer, record);
                } catch (Exception e) {
                    logger.error("failed to process record: {}", record, e);
                }
            }
            transferRepository.save(transfer);
        } finally {
            MDC.clear();
        }
    }
}