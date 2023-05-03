package hu.dpc.phee.operator.streams;

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
import org.apache.commons.text.StringEscapeUtils;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathFactory;
import java.io.StringReader;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    private DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
    private XPathFactory xPathFactory = XPathFactory.newInstance();


    public Pair<String, String> retrieveTenant(DocumentContext record) {
        String bpmnProcessIdWithTenant = record.read("$.value.bpmnProcessId", String.class);
        String[] split = bpmnProcessIdWithTenant.split("-");
        if (split.length < 2) {
            throw new RuntimeException("Invalid bpmnProcessId, has no tenant information: '" + bpmnProcessIdWithTenant + "'");
        }
        return Pair.of(split[0], split[1]);
    }

    public Transfer retrieveOrCreateTransfer(String bpmn, DocumentContext record) {
        Long processInstanceKey = record.read("$.value.processInstanceKey", Long.class);

        Transfer transfer = transferRepository.findByWorkflowInstanceKey(processInstanceKey);
        if (transfer == null) {
            logger.debug("creating new Transfer for processInstanceKey: {}", processInstanceKey);
            transfer = new Transfer(processInstanceKey);
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

    public void process(String bpmn, String tenantName, Transfer transfer, String rawData) {
        DocumentContext record = JsonPathReader.parse(rawData);
        logger.info("from kafka: {}", record.jsonString());

        String valueType = record.read("$.valueType", String.class);
        logger.debug("processing {} event", valueType);

        Long workflowKey = record.read("$.value.processDefinitionKey");
        Long workflowInstanceKey = record.read("$.value.processInstanceKey");
        Long timestamp = record.read("$.timestamp");

        List<Object> entities = switch (valueType) {
            case "DEPLOYMENT", "VARIABLE_DOCUMENT", "WORKFLOW_INSTANCE" -> List.of();
            case "PROCESS_INSTANCE" -> {
                String recordType = record.read("$.recordType", String.class);
                String intent = record.read("$.intent", String.class);
                if ("EVENT".equals(recordType)) {
                    switch (intent) {
                        case "ELEMENT_ACTIVATED" -> {
                            transfer.setStartedAt(new Date(timestamp));

                            List<TransferTransformerConfig.Transformer> constantTransformers = transferTransformerConfig.getFlows().stream()
                                    .filter(it -> bpmn.equalsIgnoreCase(it.getName()))
                                    .flatMap(it -> it.getTransformers().stream())
                                    .filter(it -> Strings.isNotBlank(it.getConstant()))
                                    .toList();

                            logger.debug("found {} constant transformers for flow start {}", constantTransformers.size(), bpmn);
                            constantTransformers.forEach(it -> applyTransformer(transfer, null, null, it));
                        }
                        case "ELEMENT_COMPLETED" -> {
                            logger.info("finishing transfer for processInstanceKey: {}", workflowInstanceKey);
                            transfer.setCompletedAt(new Date(timestamp));
                            transfer.setStatus(TransferStatus.COMPLETED);
                        }
                    }
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
                            .withElementId(record.read("$.value.elementId", String.class))
            );

            case "VARIABLE" -> {
                String variableName = record.read("$.value.name", String.class);
                String variableValue = record.read("$.value.value", String.class);
                String value = variableValue.startsWith("\"") && variableValue.endsWith("\"") ? StringEscapeUtils.unescapeJson(variableValue.substring(1, variableValue.length() - 1)) : variableValue;

                List<Object> results = List.of(
                        new Variable()
                                .withWorkflowInstanceKey(workflowInstanceKey)
                                .withName(variableName)
                                .withWorkflowKey(workflowKey)
                                .withTimestamp(timestamp)
                                .withValue(variableValue));

                logger.debug("finding transformers for bpmn: {} and variable: {}", bpmn, variableName);
                List<TransferTransformerConfig.Transformer> matchingTransformers = transferTransformerConfig.getFlows().stream()
                        .filter(it -> bpmn.equalsIgnoreCase(it.getName()))
                        .flatMap(it -> it.getTransformers().stream())
                        .filter(it -> variableName.equalsIgnoreCase(it.getVariableName()))
                        .toList();

                matchingTransformers.forEach(transformer -> applyTransformer(transfer, variableName, value, transformer));

                yield results;
            }

            case "INCIDENT" -> {
                logger.warn("TODO: not processing INCIDENT record for now");
                yield List.of();
            }

            default -> throw new IllegalStateException("Unexpected event type: " + valueType);
        };

        if (entities.size() != 0) {
            logger.info("Saving {} entities", entities.size());
            entities.forEach(entity -> {
                switch (entity) {
                    case Variable variable -> variableRepository.save(variable);
                    case Task task -> taskRepository.save(task);
                    default -> throw new IllegalStateException("Unexpected entity type: " + entity.getClass());
                }
            });
            transferRepository.save(transfer);
        }
    }

    private void applyTransformer(Transfer transfer, String variableName, String variableValue, TransferTransformerConfig.Transformer transformer) {
        logger.debug("applying transformer for field: {}", transformer.getField());
        try {
            String fieldName = transformer.getField();
            if (Strings.isNotBlank(transformer.getConstant())) {
                logger.debug("setting constant value: {} for variable {}", transformer.getConstant(), variableName);
                PropertyAccessorFactory.forBeanPropertyAccess(transfer).setPropertyValue(fieldName, transformer.getConstant());
                return;
            }

            if (Strings.isNotBlank(transformer.getJsonPath())) {
                logger.debug("applying jsonpath for variable {}", variableName);
                DocumentContext json = JsonPathReader.parse(variableValue);
                Object result = json.read(transformer.getJsonPath());
                logger.debug("jsonpath result: {} for variable {}", result, variableName);
                if (result != null) {
                    String value = switch (result) {
                        case String string -> string;
                        case List list -> list.stream().map(Object::toString).collect(Collectors.joining(" ")).toString();
                        default -> result.toString();
                    };
                    PropertyAccessorFactory.forBeanPropertyAccess(transfer).setPropertyValue(fieldName, value);
                } else {
                    logger.error("null result when setting field {} from variable {}. Jsonpath: {}, variable value: {}", fieldName, variableName, transformer.getJsonPath(), variableValue);
                }
                return;
            }

            if (Strings.isNotBlank(transformer.getXpath())) {
                logger.debug("applying xpath for variable {}", variableName);
                Document document = documentBuilderFactory.newDocumentBuilder().parse(new InputSource(new StringReader(variableValue)));
                String result = xPathFactory.newXPath().compile(transformer.getXpath()).evaluate(document);
                logger.debug("xpath result: {} for variable {}", result, variableName);
                if (result != null) {
                    PropertyAccessorFactory.forBeanPropertyAccess(transfer).setPropertyValue(fieldName, result);
                } else {
                    logger.error("null result when setting field {} from variable {}. Xpath: {}, variable value: {}", fieldName, variableName, transformer.getXpath(), variableValue);
                }
                return;
            }

            logger.debug("setting simple variable value: {} for variable {}", variableValue, variableName);
            PropertyAccessorFactory.forBeanPropertyAccess(transfer).setPropertyValue(fieldName, variableValue);

        } catch (Exception e) {
            logger.error("failed to apply transformer {} to variable {}", transformer, variableName, e);
        }
    }
}
