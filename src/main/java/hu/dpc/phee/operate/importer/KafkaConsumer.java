package hu.dpc.phee.operate.importer;

import com.jayway.jsonpath.DocumentContext;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class KafkaConsumer implements ConsumerSeekAware {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Value("${importer.kafka.topic}")
    private String kafkaTopic;

    @Autowired
    private JsonParser jsonParser;

    @Autowired
    private RecordParser recordParser;


    @KafkaListener(topics = "${importer.kafka.topic}")
    public void listen(String rawData) {
        DocumentContext json = jsonParser.parse(rawData);
        logger.debug("from kafka: {}", json.jsonString());

        String valueType = json.read("$.valueType");
        if ("VARIABLE".equals(valueType)) {
            recordParser.parseVariable(json);

        } else {
            recordParser.parseTask(json);
        }
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
