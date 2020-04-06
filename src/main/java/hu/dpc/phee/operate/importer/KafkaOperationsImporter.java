package hu.dpc.phee.operate.importer;

import org.apache.kafka.common.TopicPartition;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class KafkaOperationsImporter implements ConsumerSeekAware {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Value("${importer.kafka.topic}")
    private String kafkaTopic;

    @KafkaListener(topics = "${importer.kafka.topic}")
    public void listen(String rawData) {
        JSONObject data = new JSONObject(rawData);
        logger.info("from kafka: {}", data.toString(2));

        String intent = data.getString("intent");
        String recordType = data.getString("recordType");
        String type = null;
        if (data.has("value")) {
            JSONObject value = data.getJSONObject("value");
            if (value.has("type")) {
                type = value.getString("type");
            }
        }

        if (type != null) {
            logger.debug("{}", data.toString(2));
            logger.info("{} {} {}", intent, recordType, type);
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
