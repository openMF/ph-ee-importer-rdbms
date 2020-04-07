package hu.dpc.phee.operate.importer;

import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operate.importer.persistence.Task;
import hu.dpc.phee.operate.importer.persistence.TaskRepository;
import hu.dpc.phee.operate.importer.persistence.Transaction;
import hu.dpc.phee.operate.importer.persistence.TransactionRepository;
import hu.dpc.phee.operate.importer.persistence.Variable;
import hu.dpc.phee.operate.importer.persistence.VariableRepository;
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
import java.util.Set;

@Component
public class KafkaConsumer implements ConsumerSeekAware {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Value("${importer.kafka.topic}")
    private String kafkaTopic;

    @Value("${importer.database.max-batch-size}")
    private int maxBatchSize;

    @Autowired
    private JsonParser jsonParser;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private VariableRepository variableRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    @KafkaListener(topics = "${importer.kafka.topic}")
    public void listen(String rawData) {
        DocumentContext json = jsonParser.parse(rawData);
        logger.debug("from kafka: {}", json.jsonString());

        String valueType = json.read("$.valueType");
        Long workflowInstanceKey = json.read("$.value.workflowInstanceKey");
        Long timestamp = json.read("$.timestamp");
        if ("VARIABLE".equals(valueType)) {
            Variable variable = new Variable();
            variable.setWorkflowInstanceKey(workflowInstanceKey);
            variable.setWorkflowKey(json.read("$.value.workflowKey"));
            variable.setTimestamp(timestamp);
            String name = json.read("$.value.name");
            variable.setName(name);
            String value = json.read("$.value.value");
            variable.setValue(value);

            List<String> businessIds = Arrays.asList("transactionId", "partyId");
            if (businessIds.contains(name)) {
                Transaction transaction = new Transaction();
                transaction.setTimestamp(timestamp);
                transaction.setWorkflowInstanceKey(workflowInstanceKey);
                transaction.setBusinessKey(value);
                transaction.setBusinessKeyType(name);
                transactionRepository.save(transaction);
            }
            variableRepository.save(variable);
            return;
        }

        String intent = json.read("$.intent");
        String recordType = json.read("$.recordType");
        String type = json.read("$.value.type");

        if (type == null) {
            return;
        }

        logger.info("{} {} {}", intent, recordType, type);

        Task task = new Task();
        task.setWorkflowInstanceKey(workflowInstanceKey);
        task.setWorkflowKey(json.read("$.value.workflowKey"));
        task.setTimestamp(timestamp);
        task.setIntent(intent);
        task.setRecordType(recordType);
        task.setType(type);
        taskRepository.save(task);
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        assignments.keySet().stream()
                .filter(partition -> partition.topic().equals(kafkaTopic))
                .forEach(partition -> {
                    callback.seekToBeginning(partition.topic(), partition.partition());
                    logger.info("seeked {} to beginning", partition);
                });
    }
}
