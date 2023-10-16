package hu.dpc.phee.operator.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import hu.dpc.phee.operator.config.TransferTransformerConfig;
import hu.dpc.phee.operator.entity.batch.Batch;
import hu.dpc.phee.operator.entity.batch.BatchRepository;
import hu.dpc.phee.operator.entity.outboundmessages.OutboudMessages;
import hu.dpc.phee.operator.entity.outboundmessages.OutboundMessagesRepository;
import hu.dpc.phee.operator.entity.task.Task;
import hu.dpc.phee.operator.entity.transactionrequest.TransactionRequest;
import hu.dpc.phee.operator.entity.transactionrequest.TransactionRequestRepository;
import hu.dpc.phee.operator.entity.transactionrequest.TransactionRequestState;
import hu.dpc.phee.operator.entity.transfer.Transfer;
import hu.dpc.phee.operator.entity.transfer.TransferRepository;
import hu.dpc.phee.operator.entity.transfer.TransferStatus;
import hu.dpc.phee.operator.entity.variable.Variable;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.io.StringReader;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static hu.dpc.phee.operator.util.OperatorUtils.strip;

@Component
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
    TransferRepository transferRepository;

    @Autowired
    TransactionRequestRepository transactionRequestRepository;

    @Autowired
    BatchRepository batchRepository;

    @Autowired
    OutboundMessagesRepository outboundMessagesRepository;

    @Autowired
    TransferTransformerConfig transferTransformerConfig;

    private final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();

    private final XPathFactory xPathFactory = XPathFactory.newInstance();

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Transactional
    public List<Object> processWorkflowInstance(DocumentContext recordDocument, String bpmn, Long workflowInstanceKey, Long timestamp, String bpmnElementType, String elementId, String flowType, DocumentContext sample) {
        logger.info("Processing workflow instance");
        String recordType = recordDocument.read("$.recordType", String.class);
        String intent = recordDocument.read("$.intent", String.class);
        Optional<TransferTransformerConfig.Flow> config = transferTransformerConfig.findFlow(bpmn);

        List<TransferTransformerConfig.Transformer> constantTransformers = transferTransformerConfig.getFlows().stream()
                .filter(it -> bpmn.equalsIgnoreCase(it.getName()))
                .flatMap(it -> it.getTransformers().stream())
                .filter(it -> Strings.isNotBlank(it.getConstant()))
                .toList();

        if ("TRANSFER".equalsIgnoreCase(flowType)) {
            logger.info("Processing flow of type TRANSFER");
            Transfer transfer = inFlightTransferManager.retrieveOrCreateTransfer(bpmn, sample);
            if ("EVENT".equals(recordType) && "START_EVENT".equals(bpmnElementType) && "ELEMENT_ACTIVATED".equals(intent)) {
                transfer.setStartedAt(new Date(timestamp));
                transfer.setDirection(config.get().getDirection());
                logger.debug("found {} constant transformers for flow start {}", constantTransformers.size(), bpmn);
            } else if ("EVENT".equals(recordType) && "END_EVENT".equals(bpmnElementType) && "ELEMENT_COMPLETED".equals(intent)) {
                logger.info("finishing transfer for processInstanceKey: {} at elementId: {}", workflowInstanceKey, elementId);
                transfer.setCompletedAt(new Date(timestamp));
                if (StringUtils.isNotEmpty(elementId) && elementId.contains("Failed")) {
                    transfer.setStatus(TransferStatus.FAILED);
                } else {
                    transfer.setStatus(TransferStatus.COMPLETED);
                }
            }
            constantTransformers.forEach(it -> applyTransformer(transfer, null, null, it));
            transferRepository.save(transfer);
        } else if ("TRANSACTION-REQUEST".equalsIgnoreCase(flowType)) {
            logger.info("Processing flow of type TRANSACTION");
            TransactionRequest transactionRequest = inflightTransactionRequestManager.retrieveOrCreateTransaction(bpmn, sample);
            if ("ELEMENT_ACTIVATING".equals(intent)) {
                transactionRequest.setStartedAt(new Date(timestamp));
                transactionRequest.setDirection(config.get().getDirection());
                logger.debug("found {} constant transformers for flow start {}", constantTransformers.size(), bpmn);
            } else if ("ELEMENT_COMPLETED".equals(intent)) {
                logger.info("finishing transaction for processInstanceKey: {} at elementId: {}", workflowInstanceKey, elementId);
                transactionRequest.setCompletedAt(new Date(timestamp));
                if (StringUtils.isNotEmpty(elementId) && elementId.contains("Failed")) {
                    transactionRequest.setState(TransactionRequestState.FAILED);
                } else {
                    transactionRequest.setState(TransactionRequestState.ACCEPTED);
                }
            }
            constantTransformers.forEach(it -> applyTransformer(transactionRequest, null, null, it));
            transactionRequestRepository.save(transactionRequest);
        } else if ("BATCH".equalsIgnoreCase(flowType)) {
            logger.info("Processing flow of type BATCH");
            Batch batch = inflightBatchManager.retrieveOrCreateBatch(bpmn, sample);
            if ("ELEMENT_ACTIVATING".equals(intent)) {
                batch.setStartedAt(new Date(timestamp));
                logger.debug("found {} constant transformers for flow start {}", constantTransformers.size(), bpmn);
            } else if ("ELEMENT_COMPLETED".equals(intent)) {
                if (!config.get().getName().equalsIgnoreCase("bulk_processor")) {
                    logger.info("Inside if condition PROCESS_INSTANCE, json {}", recordType);
                    inflightBatchManager.checkWorkerIdAndUpdateTransferData(batch, workflowInstanceKey, timestamp);
                }
                batch.setCompletedAt(new Date(timestamp));
            }
            constantTransformers.forEach(it -> applyTransformer(batch, null, null, it));
            batchRepository.save(batch);
        } else if ("OUTBOUND_MESSAGES".equalsIgnoreCase(flowType)) {
            logger.info("Processing flow of type OUTBOUND MESSAGES");
            OutboudMessages outboudMessages = inflightOutboundMessageManager.retrieveOrCreateOutboundMessage(bpmn, recordDocument);
            if ("ELEMENT_ACTIVATING".equals(intent)) {
                outboudMessages.setSubmittedOnDate(new Date(timestamp));
            } else if ("ELEMENT_COMPLETED".equals(intent)) {
                outboudMessages.setDeliveredOnDate(new Date(timestamp));
            }
            outboundMessagesRepository.save(outboudMessages);
            constantTransformers.forEach(it -> applyTransformer(outboudMessages, null, null, it));
        } else {
            logger.error("No matching flow types for the given request");
        }
        return List.of();
    }

    public List<Object> processVariable(DocumentContext recordDocument, String bpmn, Long workflowInstanceKey, Long workflowKey, Long timestamp, String flowType, DocumentContext sample) {
        logger.info("Processing variable instance");
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

        matchTransformerForFlowType(flowType, bpmn, sample, matchingTransformers, variableName, value, workflowInstanceKey);

        return results;
    }

    @Transactional
    private void matchTransformerForFlowType(String flowType, String bpmn, DocumentContext sample, List<TransferTransformerConfig.Transformer> matchingTransformers, String variableName, String value, Long workflowInstanceKey) {
        Optional<TransferTransformerConfig.Flow> config = transferTransformerConfig.findFlow(bpmn);
        if ("TRANSFER".equalsIgnoreCase(flowType)) {
            Transfer transfer = inFlightTransferManager.retrieveOrCreateTransfer(bpmn, sample);
            matchingTransformers.forEach(transformer -> applyTransformer(transfer, variableName, value, transformer));
            transferRepository.save(transfer);
        } else if ("TRANSACTION-REQUEST".equalsIgnoreCase(flowType)) {
            TransactionRequest transactionRequest = inflightTransactionRequestManager.retrieveOrCreateTransaction(bpmn, sample);
            matchingTransformers.forEach(transformer -> applyTransformer(transactionRequest, variableName, value, transformer));
            transactionRequestRepository.save(transactionRequest);
        } else if ("BATCH".equalsIgnoreCase(flowType)) {
            Batch batch = inflightBatchManager.retrieveOrCreateBatch(bpmn, sample);
            matchingTransformers.forEach(transformer -> applyTransformer(batch, variableName, value, transformer));
            batchRepository.save(batch);
            if (!config.get().getName().equalsIgnoreCase("bulk_processor")) {
                logger.info("Inside if condition {}", variableName);
                if (variableName.equals("filename")) {
                    logger.info("store filename {} in tempDocStore for instance {}", strip(value), workflowInstanceKey);
                    inflightBatchManager.storeBatchFileName(workflowInstanceKey, value);
                }
                if (variableName.equals("batchId")) {
                    logger.info("store batchid {} in tempDocStore for instance {}", strip(value), workflowInstanceKey);
                    inflightBatchManager.storeBatchId(workflowInstanceKey, value);
                }
            }
        } else if ("OUTBOUND_MESSAGES".equalsIgnoreCase(flowType)) {
            OutboudMessages outboudMessages = inflightOutboundMessageManager.retrieveOrCreateOutboundMessage(bpmn, sample);
            matchingTransformers.forEach(transformer -> applyTransformer(outboudMessages, variableName, value, transformer));
            outboundMessagesRepository.save(outboudMessages);
        } else {
            logger.error("No matching flow types for the given request");
        }
    }

    public List<Object> processTask(DocumentContext recordDocument, Long workflowInstanceKey, String valueType, Long workflowKey, Long timestamp) {
        logger.info("Processing task instance");
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

    @Transactional
    public List<Object> processIncident(Long timestamp, String flowType, String bpmn, DocumentContext sample, Long workflowInstanceKey) {
        logger.info("Processing incident instance");
        if ("TRANSFER".equalsIgnoreCase(flowType)) {
            Transfer transfer = inFlightTransferManager.retrieveOrCreateTransfer(bpmn, sample);
            logger.warn("failing Transfer {} based on incident event", transfer.getTransactionId());
            transfer.setStatus(TransferStatus.EXCEPTION);
            transfer.setCompletedAt(new Date(timestamp));
            transferRepository.save(transfer);
        } else if ("TRANSACTION-REQUEST".equalsIgnoreCase(flowType)) {
            TransactionRequest transactionRequest = inflightTransactionRequestManager.retrieveOrCreateTransaction(bpmn, sample);
            logger.warn("failing Transaction {} based on incident event", transactionRequest.getTransactionId());
            transactionRequest.setState(TransactionRequestState.FAILED);
            transactionRequest.setCompletedAt(new Date(timestamp));
            transactionRequestRepository.save(transactionRequest);
        } else if ("BATCH".equalsIgnoreCase(flowType)) {
            Batch batch = inflightBatchManager.retrieveOrCreateBatch(bpmn, sample);
            logger.warn("failing Batch {} based on incident event", batch.getBatchId());
            batch.setNote("Failed Batch Request");
            batch.setCompletedAt(new Date(timestamp));
            batchRepository.save(batch);
        } else if ("OUTBOUND_MESSAGES".equalsIgnoreCase(flowType)) {
            OutboudMessages outboudMessages = inflightOutboundMessageManager.retrieveOrCreateOutboundMessage(bpmn, sample);
            logger.warn("failing Outbound Message Request {} based on incident event", outboudMessages.getInternalId());
            outboudMessages.setDeliveredOnDate(new Date(timestamp));
            outboudMessages.setDeliveryErrorMessage("Failed Message Request");
            outboundMessagesRepository.save(outboudMessages);
        } else {
            logger.error("No flow type for the incident event");
        }
        return List.of();
    }

    private void applyTransformer(Object object, String variableName, String variableValue, TransferTransformerConfig.Transformer transformer) {
        logger.debug("applying transformer for field: {}", transformer.getField());
        try {
            String fieldName = transformer.getField();
            if (Strings.isNotBlank(transformer.getConstant())) {
                logger.debug("setting constant value: {}", transformer.getConstant());
                PropertyAccessorFactory.forBeanPropertyAccess(object).setPropertyValue(fieldName, transformer.getConstant());
                return;
            }

            if (Strings.isNotBlank(transformer.getJsonPath())) {
                try {
                    ObjectMapper objectMapper = new ObjectMapper();
                    JsonNode jsonNode = objectMapper.readTree(variableValue);
                    if (jsonNode.isArray()) {
                        // It's a JSON array
                        for (JsonNode jsonObject : jsonNode) {
                            processJsonObject(jsonObject,transformer,object,fieldName,variableName,variableValue);
                        }
                    } else if (jsonNode.isObject()) {
                        // It's a JSON object
                        processJsonObject(jsonNode,transformer,object,fieldName,variableName,variableValue);
                    } else {
                        System.err.println("Invalid JSON input.");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return;
            }

            if (Strings.isNotBlank(transformer.getXpath())) {
                logger.debug("applying xpath for variable {}", variableName);
                Document document = documentBuilderFactory.newDocumentBuilder().parse(new InputSource(new StringReader(variableValue)));
                String result = xPathFactory.newXPath().compile(transformer.getXpath()).evaluate(document);
                logger.debug("xpath result: {} for variable {}", result, variableName);
                if (StringUtils.isNotBlank(result)) {
                    PropertyAccessorFactory.forBeanPropertyAccess(object).setPropertyValue(fieldName, result);
                } else {
                    logger.error("null result when setting field {} from variable {}. Xpath: {}, variable value: {}", fieldName, variableName, transformer.getXpath(), variableValue);
                }
                return;
            }

            logger.debug("setting simple variable value: {} for variable {}", variableValue, variableName);
            PropertyAccessorFactory.forBeanPropertyAccess(object).setPropertyValue(fieldName, variableValue);

        } catch (Exception e) {
            logger.error("failed to apply transformer {} to variable {}", transformer, variableName, e);
        }
    }

    private void processJsonObject(JsonNode jsonNode, TransferTransformerConfig.Transformer transformer, Object object, String fieldName, String variableName, String variableValue) {
        DocumentContext document = JsonPath.parse(jsonNode.toString());
        Object result = document.read(transformer.getJsonPath());
        logger.info("Results:{}",result);

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
            PropertyAccessorFactory.forBeanPropertyAccess(object).setPropertyValue(fieldName, value);
        }

        if (StringUtils.isBlank(value)) {
            logger.error("null result when setting field {} from variable {}. Jsonpath: {}, variable value: {}", fieldName, variableName, transformer.getJsonPath(), variableValue);
        }
    }
}
