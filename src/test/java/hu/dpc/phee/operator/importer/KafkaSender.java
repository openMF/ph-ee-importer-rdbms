package hu.dpc.phee.operator.importer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

public class KafkaSender {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Test
    public void test() {
        Properties config = new Properties();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(config);
        int oneKey = (int) (Math.random() * 1000000);
        int otherKey = (int) (Math.random() * 1000000);
        List<ProducerRecord<String, String>> records = Stream.of(
                new ProducerRecord<>("zeebe-export", "key-" + oneKey, "testOne-1"),
                null,
                new ProducerRecord<>("zeebe-export", "key-" + otherKey, "testOther-1"),
                null,
                new ProducerRecord<>("zeebe-export", "key-" + oneKey, "testOne-2"),
                new ProducerRecord<>("zeebe-export", "key-" + oneKey, "testOne-3"),
                null,
                new ProducerRecord<>("zeebe-export", "key-" + otherKey, "testOther-2")
        ).toList();

        for (ProducerRecord<String, String> record : records) {
            if (record == null) {
                sleep();
            } else {
                producer.send(record);
            }
        }
        producer.close();
        logger.info("records sent");
    }

    private void sleep() {
        try {
            Thread.sleep(1600);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
