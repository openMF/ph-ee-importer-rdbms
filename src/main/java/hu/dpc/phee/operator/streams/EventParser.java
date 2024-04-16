package hu.dpc.phee.operator.streams;

import com.baasflow.commons.events.EventLogLevel;
import com.baasflow.commons.events.EventService;
import com.baasflow.commons.events.EventStatus;
import com.baasflow.commons.events.EventType;
import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operator.config.TransferTransformerConfig;
import hu.dpc.phee.operator.entity.task.Task;
import hu.dpc.phee.operator.entity.task.TaskRepository;
import hu.dpc.phee.operator.entity.transfer.Transfer;
import hu.dpc.phee.operator.entity.transfer.TransferRepository;
import hu.dpc.phee.operator.entity.transfer.TransferStatus;
import hu.dpc.phee.operator.entity.variable.Variable;
import hu.dpc.phee.operator.entity.variable.VariableRepository;
import hu.dpc.phee.operator.importer.JsonPathReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathFactory;
import java.io.StringReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

@Component
public class EventParser {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    VariableRepository variableRepository;

    @Autowired
    TaskRepository taskRepository;

    @Autowired
    TransferRepository transferRepository;

    @Autowired
    TransferTransformerConfig transferTransformerConfig;

    @Autowired
    EventService eventService;

    private DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
    private XPathFactory xPathFactory = XPathFactory.newInstance();


    public Pair<String, String> retrieveTenant(DocumentContext record) {
        String bpmnProcessIdWithTenant = findBpmnProcessId(record);
        if (ObjectUtils.isEmpty(bpmnProcessIdWithTenant)) {
            throw new RuntimeException("can't find bpmnProcessId in record: " + record.jsonString());
        }

        String[] split = bpmnProcessIdWithTenant.split("-");
        if (split.length < 2) {
            throw new RuntimeException("Invalid bpmnProcessId, has no tenant information: '" + bpmnProcessIdWithTenant + "' in record: " + record.jsonString());
        }
        return Pair.of(split[0], split[1]);
    }

    private String findBpmnProcessId(DocumentContext record) {
        String bpmnProcessIdWithTenant = record.read("$.value.bpmnProcessId", String.class);
        if (bpmnProcessIdWithTenant == null) {
            logger.warn("can't find bpmnProcessId in record: {}, trying alternative ways..", record.jsonString());
            List<String> ids = record.read("$.value..bpmnProcessId", List.class);
            ids = ids.stream().filter(Objects::nonNull).toList();
            if (ids.size() != 1) {
                throw new RuntimeException("Invalid bpmnProcessIdWithTenant, has " + ids.size() + " bpmnProcessIds: '" + ids + "' in record: " + record.jsonString());
            }
            bpmnProcessIdWithTenant = ids.get(0);
        }
        logger.debug("resolved bpmnProcessIdWithTenant: {}", bpmnProcessIdWithTenant);
        return bpmnProcessIdWithTenant;
    }

    public Transfer retrieveOrCreateTransfer(String bpmn, DocumentContext record) {
        Long processInstanceKey = record.read("$.value.processInstanceKey", Long.class);

        Transfer transfer = transferRepository.findByWorkflowInstanceKey(processInstanceKey);
        if (transfer == null) {
            logger.debug("creating new Transfer for processInstanceKey: {}", processInstanceKey);
            transfer = new Transfer(processInstanceKey);
            transfer.setStatus(TransferStatus.IN_PROGRESS);
            Optional<TransferTransformerConfig.Flow> config = transferTransformerConfig.findFlow(bpmn);
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

                    List<TransferTransformerConfig.Transformer> constantTransformers = transferTransformerConfig.getFlows().stream()
                            .filter(it -> bpmn.equalsIgnoreCase(it.getName()))
                            .flatMap(it -> it.getTransformers().stream())
                            .filter(it -> Strings.isNotBlank(it.getConstant()))
                            .toList();

                    logger.debug("found {} constant transformers for flow start {}", constantTransformers.size(), bpmn);
                    constantTransformers.forEach(it -> applyTransformer(transfer, null, null, it));
//                    yield List.of(new Task()
//                            .withWorkflowInstanceKey(workflowInstanceKey)
//                            .withWorkflowKey(workflowKey)
//                            .withTimestamp(timestamp)
//                            .withIntent(intent)
//                            .withRecordType(recordType)
//                            .withType("START_FLOW")
//                            .withElementId(elementId)
//                    );
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

                    yield List.of();
//                    yield List.of(new Task()
//                            .withWorkflowInstanceKey(workflowInstanceKey)
//                            .withWorkflowKey(workflowKey)
//                            .withTimestamp(timestamp)
//                            .withIntent(intent)
//                            .withRecordType(recordType)
//                            .withType("END_FLOW")
//                            .withElementId(elementId)
//                    );
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
                List<TransferTransformerConfig.Transformer> matchingTransformers = transferTransformerConfig.getFlows().stream()
                        .filter(it -> bpmn.equalsIgnoreCase(it.getName()))
                        .flatMap(it -> it.getTransformers().stream())
                        .filter(it -> variableName.equalsIgnoreCase(it.getVariableName()))
                        .toList();

                matchingTransformers.forEach(transformer -> applyTransformer(transfer, variableName, value, transformer));

                yield List.of(
                        new Variable()
                                .withWorkflowInstanceKey(workflowInstanceKey)
                                .withName(variableName)
                                .withWorkflowKey(workflowKey)
                                .withTimestamp(timestamp)
                                .withValue(value));
            }

            case "INCIDENT" -> {
                logger.warn("failing Transfer {} based on incident event", transfer.getTransactionId());
                transfer.setStatus(TransferStatus.EXCEPTION);
                transfer.setCompletedAt(new Date(timestamp));

                sendIncidentAuditlog(tenantName, transfer, record.jsonString());
                yield List.of(
                        new Variable()
                                .withWorkflowInstanceKey(workflowInstanceKey)
                                .withName("exception")
                                .withWorkflowKey(workflowKey)
                                .withTimestamp(timestamp)
                                .withValue(StringEscapeUtils.escapeJson(record.jsonString()))
                );
            }

            default -> throw new IllegalStateException("Unexpected event type: " + valueType);
        };

        if (entities.size() != 0) {
            logger.info("Saving {} entities", entities.size());
            entities.forEach(entity -> {
                if (entity instanceof Variable) {
                    variableRepository.save((Variable) entity);
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

    private void applyTransformer(Transfer transfer, String variableName, String variableValue, TransferTransformerConfig.Transformer transformer) {
        logger.debug("applying transformer for field: {}", transformer.getField());
        try {
            String fieldName = transformer.getField();
            String dateFormat = transformer.getDateFormat();
            if (Strings.isNotBlank(transformer.getConstant())) {
                logger.debug("setting constant value: {}", transformer.getConstant());
                setPropertyValue(transfer, fieldName, transformer.getConstant(), dateFormat);
                return;
            }

            if (Strings.isNotBlank(transformer.getJsonPath())) {
                logger.debug("applying jsonpath for variable {}", variableName);
                DocumentContext json = JsonPathReader.parse(variableValue);
                Object result = json.read(transformer.getJsonPath());
                logger.debug("jsonpath result: {} for variable {}", result, variableName);

                String value = null;
                if (result != null) {
                    if (result instanceof String) {
                        value = (String) result;
                    }
                    if (result instanceof List) {
                        value = ((List<?>) result).stream().map(Object::toString).collect(Collectors.joining(" "));
                    } else {
                        value = result.toString();
                    }
                    setPropertyValue(transfer, fieldName, value, dateFormat);
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
                    setPropertyValue(transfer, fieldName, result, dateFormat);
                } else {
                    logger.error("null result when setting field {} from variable {}. Xpath: {}, variable value: {}", fieldName, variableName, transformer.getXpath(), variableValue);
                }
                return;
            }

            logger.debug("setting simple variable value: {} for variable {}", variableValue, variableName);
            setPropertyValue(transfer, fieldName, variableValue, dateFormat);

        } catch (Exception e) {
            logger.error("failed to apply transformer {} to variable {}", transformer, variableName, e);
        }
    }

    private void setPropertyValue(Transfer transfer, String fieldName, String variableValue, String dateFormat) {
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
    }
}
