package hu.dpc.phee.operator.streams;

import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operator.config.TransferTransformerConfig;
import hu.dpc.phee.operator.entity.batch.Batch;
import hu.dpc.phee.operator.entity.outboundmessages.OutboudMessages;
import hu.dpc.phee.operator.entity.task.Task;
import hu.dpc.phee.operator.entity.transactionrequest.TransactionRequest;
import hu.dpc.phee.operator.entity.transactionrequest.TransactionRequestState;
import hu.dpc.phee.operator.entity.transfer.Transfer;
import hu.dpc.phee.operator.entity.transfer.TransferStatus;
import hu.dpc.phee.operator.entity.variable.Variable;
import hu.dpc.phee.operator.importer.InflightOutboundMessageManager;
import hu.dpc.phee.operator.importer.JsonPathReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathFactory;
import java.io.StringReader;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class RecordParser {

    @Autowired
    private InFlightTransferManager inFlightTransferManager;

    @Autowired
    private InflightTransactionRequestManager inflightTransactionRequestManager;

    @Autowired
    private InflightBatchManager inflightBatchManager;

    @Autowired
    private InflightOutboundMessageManager inflightOutboundMessageManager;

    @Autowired
    TransferTransformerConfig transferTransformerConfig;

    private DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();

    private XPathFactory xPathFactory = XPathFactory.newInstance();

    private Logger logger = LoggerFactory.getLogger(this.getClass());


    public List<Object> processWorkflowInstance(DocumentContext recordDocument, String bpmn, Long workflowInstanceKey, Long timestamp, String bpmnElementType, String elementId, String flowType) {
        String recordType = recordDocument.read("$.recordType", String.class);
        String intent = recordDocument.read("$.intent", String.class);

        if ("EVENT".equals(recordType) && "START_EVENT".equals(bpmnElementType) && "ELEMENT_ACTIVATED".equals(intent)) {
            transfer.setStartedAt(new Date(timestamp));

            List<TransferTransformerConfig.Transformer> constantTransformers = transferTransformerConfig.getFlows().stream()
                    .filter(it -> bpmn.equalsIgnoreCase(it.getName()))
                    .flatMap(it -> it.getTransformers().stream())
                    .filter(it -> Strings.isNotBlank(it.getConstant()))
                    .toList();

            logger.debug("found {} constant transformers for flow start {}", constantTransformers.size(), bpmn);
            constantTransformers.forEach(it -> applyTransformer(transfer, null, null, it));
        }

        if ("EVENT".equals(recordType) && "END_EVENT".equals(bpmnElementType) && "ELEMENT_COMPLETED".equals(intent)) {
            logger.info("finishing transfer for processInstanceKey: {} at elementId: {}", workflowInstanceKey, elementId);
            transfer.setCompletedAt(new Date(timestamp));
            if (StringUtils.isNotEmpty(elementId) && elementId.contains("Failed")) {
                transfer.setStatus(TransferStatus.FAILED);
            } else {
                transfer.setStatus(TransferStatus.COMPLETED);
            }
        }

        return List.of();
    }

    public List<Object> processVariable(DocumentContext recordDocument, String bpmn, Long workflowInstanceKey, Long workflowKey, Long timestamp, String flowType) {
        String variableName = recordDocument.read("$.value.name", String.class);
        String variableValue = recordDocument.read("$.value.value", String.class);
        String value = variableValue.startsWith("\"") && variableValue.endsWith("\"") ? StringEscapeUtils.unescapeJson(variableValue.substring(1, variableValue.length() - 1)) : variableValue;

        List<Object> results = List.of(
                new Variable()
                        .withWorkflowInstanceKey(workflowInstanceKey)
                        .withName(variableName)
                        .withWorkflowKey(workflowKey)
                        .withTimestamp(timestamp)
                        .withValue(value));

        logger.debug("finding transformers for bpmn: {} and variable: {}", bpmn, variableName);
        List<TransferTransformerConfig.Transformer> matchingTransformers = transferTransformerConfig.getFlows().stream()
                .filter(it -> bpmn.equalsIgnoreCase(it.getName()))
                .flatMap(it -> it.getTransformers().stream())
                .filter(it -> variableName.equalsIgnoreCase(it.getVariableName()))
                .toList();

        matchingTransformers.forEach(transformer -> applyTransformer(transfer, variableName, value, transformer));

        return results;
    }

    public List<Object> processTask(DocumentContext recordDocument, Long workflowInstanceKey, String valueType, Long workflowKey, Long timestamp) {
        return List.of(
                new Task()
                        .withWorkflowInstanceKey(workflowInstanceKey)
                        .withWorkflowKey(workflowKey)
                        .withTimestamp(timestamp)
                        .withIntent(recordDocument.read("$.intent", String.class))
                        .withRecordType(valueType)
                        .withType(recordDocument.read("$.value.type", String.class))
                        .withElementId(recordDocument.read("$.value.elementId", String.class))
        );
    }

    public List<Object> processIncident(Long timestamp, String flowType, String bpmn, DocumentContext sample) {
        if ("TRANSFER".equalsIgnoreCase(flowType)) {
            Transfer transfer = inFlightTransferManager.retrieveOrCreateTransfer(bpmn, sample);
            logger.warn("failing Transfer {} based on incident event", transfer.getTransactionId());
            transfer.setStatus(TransferStatus.EXCEPTION);
            transfer.setCompletedAt(new Date(timestamp));
        } else if ("TRANSACTION-REQUEST".equalsIgnoreCase(flowType)) {
            TransactionRequest transactionRequest = inflightTransactionRequestManager.retrieveOrCreateTransaction(bpmn, sample);
            logger.warn("failing Transaction {} based on incident event", transactionRequest.getTransactionId());
            transactionRequest.setState(TransactionRequestState.FAILED);
            transactionRequest.setCompletedAt(new Date(timestamp));
        } else if ("BATCH".equalsIgnoreCase(flowType)) {
            Batch batch = inflightBatchManager.retrieveOrCreateBatch(bpmn, sample);
            logger.warn("failing Batch {} based on incident event", batch.getBatchId());
            batch.setNote("Failed Batch Request");
            batch.setCompletedAt(new Date(timestamp));
        } else if ("OUTBOUND_MESSAGES".equalsIgnoreCase(flowType)) {
            OutboudMessages outboudMessages = inflightOutboundMessageManager.getOrCreateOutboundMessage();
            logger.warn("failing Outbound Message Request {} based on incident event", batch.getBatchId());
            outboudMessages.setDeliveryErrorMessage("Failed Message Request");
            outboudMessages.setDeliveredOnDate(new Date(timestamp));
        } else {
            logger.error("No flow type for the incident event");
        }
        return List.of();
    }

    private void applyTransformer(Transfer transfer, String variableName, String variableValue, TransferTransformerConfig.Transformer transformer) {
        logger.debug("applying transformer for field: {}", transformer.getField());
        try {
            String fieldName = transformer.getField();
            if (Strings.isNotBlank(transformer.getConstant())) {
                logger.debug("setting constant value: {}", transformer.getConstant());
                PropertyAccessorFactory.forBeanPropertyAccess(transfer).setPropertyValue(fieldName, transformer.getConstant());
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
                    PropertyAccessorFactory.forBeanPropertyAccess(transfer).setPropertyValue(fieldName, value);
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
