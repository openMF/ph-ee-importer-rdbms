package hu.dpc.phee.operator.importer;

import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operator.business.TransactionDirection;
import hu.dpc.phee.operator.business.TransactionStatus;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Component
public class KafkaConsumer implements ConsumerSeekAware {
    private static final String OUTGOING_BPMN_NAME = "PaymentTest3";
    private static final String INCOMING_BPMN_NAME = "PayeeQuoteTransfer-DFSPID";
    private static final List<String> TRANSFER_BPMN_NAMES = Arrays.asList(INCOMING_BPMN_NAME, OUTGOING_BPMN_NAME);

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Value("${importer.kafka.topic}")
    private String kafkaTopic;

    @Value("${importer.kafka.reset}")
    private boolean reset;

    @Autowired
    private RecordParser recordParser;

    @Autowired
    private OutgoingVariableParser outgoingVariableParser;

    @Autowired
    private IncomingVariableParser incomingVariableParser;

    @Autowired
    private TransactionManager transactionManager;


    @KafkaListener(topics = "${importer.kafka.topic}")
    public void listen(String rawData) {
        DocumentContext json = JsonPathReader.parse(rawData);
        logger.debug("from kafka: {}", json.jsonString());

        String valueType = json.read("$.valueType");
        switch (valueType) {
            case "VARIABLE":
                processVariable(json);
                break;

            case "JOB":
                processJob(json);
                break;

            case "WORKFLOW_INSTANCE":
                processWorkflowInstance(json);
                break;
        }
    }

    private void processVariable(DocumentContext json) {
        recordParser.parseVariable(json);
        Long workflowInstanceKey = json.read("$.value.workflowInstanceKey");

        TransactionDirection direction = transactionManager.isInflightTransaction(workflowInstanceKey);
        if (direction == TransactionDirection.OUTGOING) {
            outgoingVariableParser.parseVariable(json);

        } else if (direction == TransactionDirection.INCOMING) {
            incomingVariableParser.parseVariable(json);
        }
    }

    private void processJob(DocumentContext json) {
        recordParser.parseTask(json);
    }

    private void processWorkflowInstance(DocumentContext json) {
        String bpmnProcessId = json.read("$.value.bpmnProcessId");
        String bpmnElementType = json.read("$.value.bpmnElementType");
        String intent = json.read("$.intent");

        if (TRANSFER_BPMN_NAMES.contains(bpmnProcessId)) {
            Long workflowInstanceKey = json.read("$.value.workflowInstanceKey");

            if ("END_EVENT".equals(bpmnElementType) && "ELEMENT_COMPLETED".equals(intent)) {
                String endElementId = json.read("$.value.elementId");

                if (endElementId.startsWith("EndEvent_Success")) {
                    transactionManager.transactionResult(workflowInstanceKey, TransactionStatus.COMPLETED, endElementId);

                } else if (endElementId.startsWith("EndEvent_Fail")) {
                    transactionManager.transactionResult(workflowInstanceKey, TransactionStatus.FAILED, endElementId);

                } else {
                    transactionManager.transactionResult(workflowInstanceKey, TransactionStatus.UNKNOWN, endElementId);
                }
            }

            TransactionDirection direction = getTransactionDirection(bpmnProcessId);

            if ("PROCESS".equals(bpmnElementType)) {
                Long timestamp = json.read("$.timestamp");

                if ("ELEMENT_ACTIVATING".equals(intent)) {
                    transactionManager.processStarted(workflowInstanceKey, timestamp, direction);
                } else if ("ELEMENT_COMPLETED".equals(intent)) {
                    transactionManager.processEnded(workflowInstanceKey, timestamp, direction);
                }
            }
        }
    }

    private TransactionDirection getTransactionDirection(String bpmnProcessId) {
        if (OUTGOING_BPMN_NAME.equals(bpmnProcessId)) {
            return TransactionDirection.OUTGOING;
        } else if (INCOMING_BPMN_NAME.equals(bpmnProcessId)) {
            return TransactionDirection.INCOMING;
        } else {
            throw new AssertionError("Can't determine transaction direction from unhandled process type " + bpmnProcessId);
        }
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        if (reset) {
            assignments.keySet().stream()
                    .filter(partition -> partition.topic().equals(kafkaTopic))
                    .forEach(partition -> {
                        callback.seekToBeginning(partition.topic(), partition.partition());
                        logger.info("seeked {} to beginning", partition);
                    });
        } else {
            logger.info("no reset, consuming kafka topics from latest");
        }
    }
}
