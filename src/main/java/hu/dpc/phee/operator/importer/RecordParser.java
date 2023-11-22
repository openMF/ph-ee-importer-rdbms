package hu.dpc.phee.operator.importer;

import static hu.dpc.phee.operator.OperatorUtils.strip;

import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operator.config.BpmnProcess;
import hu.dpc.phee.operator.config.BpmnProcessProperties;
import hu.dpc.phee.operator.entity.batch.Batch;
import hu.dpc.phee.operator.entity.batch.BatchRepository;
import hu.dpc.phee.operator.entity.batch.Transaction;
import hu.dpc.phee.operator.entity.outboundmessages.OutboudMessages;
import hu.dpc.phee.operator.entity.outboundmessages.OutboundMessagesRepository;
import hu.dpc.phee.operator.entity.task.Task;
import hu.dpc.phee.operator.entity.task.TaskRepository;
import hu.dpc.phee.operator.entity.tenant.ThreadLocalContextUtil;
import hu.dpc.phee.operator.entity.transactionrequest.TransactionRequest;
import hu.dpc.phee.operator.entity.transactionrequest.TransactionRequestRepository;
import hu.dpc.phee.operator.entity.transfer.Transfer;
import hu.dpc.phee.operator.entity.transfer.TransferRepository;
import hu.dpc.phee.operator.entity.transfer.TransferStatus;
import hu.dpc.phee.operator.entity.variable.Variable;
import hu.dpc.phee.operator.entity.variable.VariableRepository;
import hu.dpc.phee.operator.file.CsvFileService;
import hu.dpc.phee.operator.file.FileTransferService;
import hu.dpc.phee.operator.util.BatchFormatToTransferMapper;
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Component;

@Component
public class RecordParser {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Value("${bpmn.transfer-type}")
    private String transferType;

    @Value("${bpmn.transaction-request-type}")
    private String transactionRequestType;

    @Value("${bpmn.batch-type}")
    private String batchType;
    @Value("${bpmn.outbound-message-type}")
    private String outboundMessageType;

    @Value("${bpmn.outgoing-direction}")
    private String outgoingDirection;

    @Value("${application.bucket-name}")
    private String bucketName;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private VariableRepository variableRepository;

    @Autowired
    private TransferRepository transferRepository;

    @Autowired
    private OutboundMessagesRepository outboundMessagesRepository;

    @Autowired
    private TransactionRequestRepository transactionRequestRepository;

    @Autowired
    private BatchRepository batchRepository;

    @Autowired
    private BpmnProcessProperties bpmnProcessProperties;

    @Autowired
    private InflightTransferManager inflightTransferManager;

    @Autowired
    private InflightTransactionRequestManager inflightTransactionRequestManager;

    @Autowired
    private InflightBatchManager inflightBatchManager;
    @Autowired
    private InflightOutboundMessageManager inflightOutboundMessageManager;

    @Autowired
    private VariableParser variableParser;

    @Autowired
    private TempDocumentStore tempDocumentStore;

    @Autowired
    private FileTransferService fileTransferService;

    @Autowired
    private CsvFileService csvFileService;

    private final Map<Long, Long> inflightCallActivities = new ConcurrentHashMap<>();

    public void addVariableToEntity(DocumentContext newVariable, String bpmnProcessId) {

        if (newVariable == null) {
            return;
        }
        logger.debug("newVariable in RecordParser: {}", newVariable.jsonString()); //
        String name = newVariable.read("$.value.name");
        Long workflowInstanceKey = newVariable.read("$.value.processInstanceKey");
        if (inflightCallActivities.containsKey(workflowInstanceKey)) {
            Long parentInstanceKey = inflightCallActivities.get(workflowInstanceKey);
            logger.debug("variable {} in instance {} has parent workflowInstance {}", name, workflowInstanceKey, parentInstanceKey);
            workflowInstanceKey = parentInstanceKey;
        }

        BpmnProcess bpmnProcess = bpmnProcessProperties.getById(bpmnProcessId);
        if (transferType.equals(bpmnProcess.getType())) {
            if (variableParser.getTransferParsers().containsKey(name)) {
                logger.debug("add variable {} to transfer for workflow {}", name, workflowInstanceKey);
                String value = newVariable.read("$.value.value");

                Transfer transfer = inflightTransferManager.getOrCreateTransfer(workflowInstanceKey);
                variableParser.getTransferParsers().get(name).accept(Pair.of(transfer, value));
                transferRepository.save(transfer);
            }
        } else if (transactionRequestType.equals(bpmnProcess.getType())) {
            if (variableParser.getTransactionRequestParsers().containsKey(name)) {
                logger.debug("add variable to transactionRequest {} for workflow {}", name, workflowInstanceKey);
                String value = newVariable.read("$.value.value");

                TransactionRequest transactionRequest = inflightTransactionRequestManager
                        .getOrCreateTransactionRequest(workflowInstanceKey);
                variableParser.getTransactionRequestParsers().get(name).accept(Pair.of(transactionRequest, value));
                if (transactionRequest.getDirection() == null) {
                    transactionRequest.setDirection(bpmnProcess.getDirection());
                }
                transactionRequestRepository.save(transactionRequest);
            }
        } else if (batchType.equals(bpmnProcess.getType())) {
            if (variableParser.getBatchParsers().containsKey(name)) {
                logger.debug("add variable {} to batch for workflow {}", name, workflowInstanceKey);
                String value = newVariable.read("$.value.value");

                Batch batch = inflightBatchManager.getOrCreateBatch(workflowInstanceKey);
                variableParser.getBatchParsers().get(name).accept(Pair.of(batch, value));
                batchRepository.save(batch);

                if (name.equals("failedTransactionFile")) {
                    // insert the transaction into transfer table
                    logger.info("failedTransactionFile case");
                    updateTransferTableWithFailedTransaction(workflowInstanceKey, value);
                }

                if (!bpmnProcess.getId().equalsIgnoreCase("bulk_processor")) {
                    logger.info("Inside if condition {}", name);
                    if (name.equals("filename")) {
                        logger.info("store filename {} in tempDocStore for instance {}", strip(value), workflowInstanceKey);
                        tempDocumentStore.storeBatchFileName(workflowInstanceKey, value);
                    }
                    if (name.equals("batchId")) {
                        logger.info("store batchid {} in tempDocStore for instance {}", strip(value), workflowInstanceKey);
                        tempDocumentStore.storeBatchId(workflowInstanceKey, value);
                    }
                }
                if (bpmnProcess.getId().equalsIgnoreCase("bulk_processor")) {
                    if (name.equals("batchId")) {
                        logger.info("store batchid {} in tempDocStore for instance {}", strip(value), workflowInstanceKey);
                        tempDocumentStore.storeBatchId(workflowInstanceKey, value);
                    }
                }
            }
        } else if (outboundMessageType.equals(bpmnProcess.getType())) {
            if (variableParser.getOutboundMessageParsers().containsKey(name)) {
                logger.debug("add variable {} to outbound messages for workflow {}", name, workflowInstanceKey);
                String value = newVariable.read("$.value.value");
                OutboudMessages outboudMessages = inflightOutboundMessageManager.getOrCreateOutboundMessage(workflowInstanceKey);
                variableParser.getOutboundMessageParsers().get(name).accept(Pair.of(outboudMessages, value));
                outboundMessagesRepository.save(outboudMessages);
            }
        } else {
            logger.debug("Skip adding variable to {} and type is {}", bpmnProcessId, bpmnProcess.getType()); // xx
        }
    }

    public DocumentContext processVariable(DocumentContext json) {
        Long workflowInstanceKey = json.read("$.value.processInstanceKey");
        String name = json.read("$.value.name");
        Long newTimestamp = json.read("$.timestamp");
        List<Variable> existingVariables = variableRepository.findByWorkflowInstanceKey(workflowInstanceKey);
        if (existingVariables != null && !existingVariables.isEmpty()) {
            if (existingVariables.stream().filter(existing -> {
                return name.equals(existing.getName()) && newTimestamp <= existing.getTimestamp(); // variable already
                                                                                                   // inserted before
            }).findFirst().orElse(null) != null) {
                logger.debug("Variable {} already inserted at {} for instance {}, skip processing!", name, newTimestamp,
                        workflowInstanceKey);
                return null;
            }
        }

        Variable variable = new Variable();
        variable.setWorkflowInstanceKey(workflowInstanceKey);
        variable.setTimestamp(newTimestamp);
        variable.setWorkflowKey(json.read("$.value.processDefinitionKey"));
        variable.setName(name);
        String value = json.read("$.value.value");
        variable.setValue(value);
        variableRepository.save(variable);
        return json;
    }

    public void processWorkflowInstance(DocumentContext json) {
        String bpmnProcessId = json.read("$.value.bpmnProcessId");
        BpmnProcess bpmnProcess = bpmnProcessProperties.getById(bpmnProcessId.split("-")[0]);
        Long workflowInstanceKey = json.read("$.value.processInstanceKey");
        Long timestamp = json.read("$.timestamp");
        String intent = json.read("$.intent");
        Object parentWorkflowInstanceKey = json.read("$.value.parentProcessInstanceKey");
        boolean hasParent = false;
        if (parentWorkflowInstanceKey instanceof Long && (Long) parentWorkflowInstanceKey > 0) {
            hasParent = true;
        }

        String elementId = json.read("$.value.elementId");
        Long callActivityKey = json.read("$.key");
        if (transferType.equals(bpmnProcess.getType())) {
            if ("ELEMENT_ACTIVATING".equals(intent)) {
                if (hasParent) {
                    logger.debug("Sub process {} with key {} started from parent instance {}", bpmnProcessId, callActivityKey,
                            parentWorkflowInstanceKey);
                    inflightCallActivities.put(callActivityKey, (Long) parentWorkflowInstanceKey);
                    inflightTransferManager.transferStarted((Long) parentWorkflowInstanceKey, timestamp, outgoingDirection);
                } else {
                    inflightTransferManager.transferStarted(workflowInstanceKey, timestamp, bpmnProcess.getDirection());
                }
            } else if ("ELEMENT_COMPLETED".equals(intent)) {
                if (inflightCallActivities.containsKey(workflowInstanceKey)) {
                    Long parentInstanceKey = inflightCallActivities.remove(workflowInstanceKey);
                    logger.debug("Sub process {} with key {} ended from parent instance {}", bpmnProcessId, callActivityKey,
                            parentInstanceKey);
                    workflowInstanceKey = parentInstanceKey;
                }
                inflightTransferManager.transferEnded(workflowInstanceKey, timestamp);
            }
        } else if (transactionRequestType.equals(bpmnProcess.getType())) {
            if ("ELEMENT_ACTIVATING".equals(intent)) {
                inflightTransactionRequestManager.transactionRequestStarted(workflowInstanceKey, timestamp, bpmnProcess.getDirection());
            } else if ("ELEMENT_COMPLETED".equals(intent)) {
                inflightTransactionRequestManager.transactionRequestEnded(workflowInstanceKey, timestamp);
            }
        } else if (batchType.equals(bpmnProcess.getType())) {
            if ("ELEMENT_ACTIVATING".equals(intent)) {
                inflightBatchManager.batchStarted(workflowInstanceKey, timestamp, bpmnProcess.getDirection());
            } else if ("ELEMENT_COMPLETED".equals(intent)) {
                if (!bpmnProcess.getId().equalsIgnoreCase("bulk_processor")) {
                    logger.info("Inside if condition PROCESS_INSTANCE, json {}", json.jsonString());
                    checkWorkerIdAndUpdateTransferData(workflowInstanceKey, timestamp);
                }
                inflightBatchManager.batchEnded(workflowInstanceKey, timestamp);
            }
        } else if (outboundMessageType.equals(bpmnProcess.getType())) {
            logger.info("---------------- ELEMENT_ACTIVATING ----------intent - {}, json - {} ", intent, json);
            if ("ELEMENT_ACTIVATING".equals(intent)) {
                inflightOutboundMessageManager.outboundMessageStarted(workflowInstanceKey, timestamp, bpmnProcess.getDirection());
            } else if ("ELEMENT_COMPLETED".equals(intent)) {
                inflightOutboundMessageManager.outboundMessageEnded(workflowInstanceKey, timestamp);
            }
        } else {
            logger.error("Skip parsing bpmnProcess: {}, bpmnProcessId: {}, document: {} as bpmn isn't set", bpmnProcess, bpmnProcessId,
                    json.jsonString());
        }
    }

    public void processTask(DocumentContext json) {
        String type = json.read("$.value.type");
        if (type == null) {
            return;
        }

        Long workflowInstanceKey = json.read("$.value.processInstanceKey");
        String newElementId = json.read("$.value.elementId");
        Long newTimestamp = json.read("$.timestamp");
        String newIntent = json.read("$.intent");
        List<Task> existingTasks = taskRepository.findByWorkflowInstanceKey(workflowInstanceKey);
        if (existingTasks != null && !existingTasks.isEmpty()) {
            if (existingTasks.stream().filter(existing -> {
                return newElementId.equals(existing.getElementId()) && newIntent.equals(existing.getIntent()); // task
                                                                                                               // intent
                                                                                                               // inserts
                                                                                                               // happens
                                                                                                               // for
                                                                                                               // only
                                                                                                               // once
            }).findFirst().orElse(null) != null) {
                logger.info("Task {} with intent {} already inserted at {} for instance {}, skip processing!", newElementId, newIntent,
                        newTimestamp, workflowInstanceKey);
                return;
            }
        }

        Task task = new Task();
        task.setWorkflowInstanceKey(workflowInstanceKey);
        task.setWorkflowKey(json.read("$.value.processDefinitionKey"));
        task.setTimestamp(newTimestamp);
        task.setIntent(newIntent);
        task.setRecordType(json.read("$.recordType"));
        task.setType(type);
        task.setElementId(newElementId);
        taskRepository.save(task);
    }

    private void checkWorkerIdAndUpdateTransferData(Long workflowInstanceKey, Long completeTimestamp) {
        updateTransferTableForBatch(workflowInstanceKey, completeTimestamp);
    }

    // reads data from csv file and write data to transfers table
    private void updateTransferTableForBatch(Long workflowInstanceKey, Long completeTimestamp) {
        String filename = tempDocumentStore.getBatchFileName(workflowInstanceKey);
        logger.info("Filename {}", filename);
        if (filename == null) {
            return;
        }
        filename = strip(filename);
        String localFilePath = fileTransferService.downloadFile(filename, bucketName);
        if (localFilePath == null) {
            logger.error("Null localFilePath, Error updating transfer table for batch with instance key {} and batch filename {}",
                    workflowInstanceKey, filename);
            return;
        }
        List<Transaction> transactionList = csvFileService.getTransactionList(filename);

        Batch batch = batchRepository.findByWorkflowInstanceKey(workflowInstanceKey);
        for (Transaction transaction : transactionList) {
            Transfer transfer = BatchFormatToTransferMapper.mapToTransferEntity(transaction);
            transfer.setWorkflowInstanceKey(workflowInstanceKey);

            String batchId = tempDocumentStore.getBatchId(workflowInstanceKey);
            transfer.setBatchId(strip(batchId));
            transfer.setCompletedAt(new Date(completeTimestamp));
            transfer.setTransactionId(transaction.getRequestId());

            transfer.setPayeeDfspId(batch.getPaymentMode());
            transfer.setPayerDfspId(ThreadLocalContextUtil.getTenant().getSchemaName());

            transfer.setPayeeFeeCurrency(transaction.getCurrency());
            transfer.setPayeeFee(BigDecimal.ZERO);
            transfer.setPayerFeeCurrency(transaction.getCurrency());
            transfer.setPayerFee(BigDecimal.ZERO);

            BatchFormatToTransferMapper.updateTransferUsingBatchDetails(transfer, batch);
            transferRepository.save(transfer);
        }
    }

    private void updateTransferTableWithFailedTransaction(Long workflowInstanceKey, String filename) {
        logger.info("Filename {}", filename);
        if (filename == null) {
            return;
        }
        filename = strip(filename);
        String localFilePath = fileTransferService.downloadFile(filename, bucketName);
        List<Transaction> transactionList = csvFileService.getTransactionList(localFilePath);
        for (Transaction transaction : transactionList) {
            Transfer transfer = BatchFormatToTransferMapper.mapToTransferEntity(transaction);
            transfer.setStatus(TransferStatus.FAILED);
            transfer.setBatchId(strip(tempDocumentStore.getBatchId(workflowInstanceKey)));
            transfer.setStartedAt(new Date());
            transfer.setCompletedAt(new Date());
            transfer.setErrorInformation(transaction.getNote());
            transfer.setClientCorrelationId(UUID.randomUUID().toString());
            transfer.setTransactionId(UUID.randomUUID().toString());
            logger.debug("Inserting failed txn: {}", transfer);
            logger.info("Inserting failed txn with note: {}", transaction.getNote());
            transferRepository.save(transfer);
        }
    }

}
