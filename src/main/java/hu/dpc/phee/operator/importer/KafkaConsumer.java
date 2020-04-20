package hu.dpc.phee.operator.importer;

import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operator.business.IncomingTransactionParser;
import hu.dpc.phee.operator.business.OutgoingTransactionParser;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
public class KafkaConsumer implements ConsumerSeekAware {
    private static final String OUTGOING_BPMN_NAME = "PaymentTest3";
    private static final List<String> INCOMING_BPMN_NAMES = Arrays.asList(IncomingTransactionParser.BPMN_PAYEE_QUOTE_TRANSFER);

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Value("${importer.kafka.topic}")
    private String kafkaTopic;

    @Value("${importer.kafka.reset}")
    private boolean reset;

    @Autowired
    private RecordParser recordParser;

    @Autowired
    private OutgoingTransactionParser outgoingTransactionParser;

    @Autowired
    private IncomingTransactionParser incomingTransactionParser;

    private Set<Long> outgoingTransactions = new HashSet<>();
    private Set<Long> incomingTransactions = new HashSet<>();


    @KafkaListener(topics = "${importer.kafka.topic}")
    public void listen(String rawData) {
        DocumentContext json = JsonPathReader.parse(rawData);
        logger.debug("from kafka: {}", json.jsonString());

        Long workflowKey = json.read("$.value.workflowKey");
        String valueType = json.read("$.valueType");
        switch (valueType) {
            case "VARIABLE":
                processVariable(json, workflowKey);
                break;

            case "JOB":
                processJob(json, workflowKey);
                break;

            case "WORKFLOW_INSTANCE":
                processWorkflowInstance(json, workflowKey);
                break;
        }
    }

    private void processVariable(DocumentContext json, Long workflowKey) {
        recordParser.parseVariable(json);

        if (outgoingTransactions.contains(workflowKey)) {
            outgoingTransactionParser.parseVariable(json);

        } else if (incomingTransactions.contains(workflowKey)) {
            incomingTransactionParser.parseVariable(json);

        } else {
            logger.debug("skipping unknown VARIABLE for workflowKey {}", workflowKey);
        }
    }

    private void processJob(DocumentContext json, Long workflowKey) {
        recordParser.parseTask(json);

        if (outgoingTransactions.contains(workflowKey)) {
            outgoingTransactionParser.checkTransactionStatus(json);

        } else if (incomingTransactions.contains(workflowKey)) {
            incomingTransactionParser.checkTransactionStatus(json);

        } else {
            logger.debug("skipping unknown JOB for workflowKey {}", workflowKey);
        }
    }

    private void processWorkflowInstance(DocumentContext json, Long workflowKey) {
        String bpmnProcessId = json.read("$.value.bpmnProcessId");
        if (OUTGOING_BPMN_NAME.equals(bpmnProcessId)) {
            outgoingTransactions.add(workflowKey);
            logger.debug("registered {} as OUTGOING workflowKey", workflowKey);
            outgoingTransactionParser.parseWorkflowElement(json);

        } else if (INCOMING_BPMN_NAMES.contains(bpmnProcessId)) {
            incomingTransactions.add(workflowKey);
            logger.debug("registered {} as INCOMING workflowKey", workflowKey);
            incomingTransactionParser.parseWorkflowElement(json);

        } else {
            logger.debug("skipping unknown WORKFLOW_INSTANCE {}", bpmnProcessId);
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
