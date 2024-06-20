package hu.dpc.phee.operator.config;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class KafkaProductionExceptionHandler extends DefaultProductionExceptionHandler {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        logger.info("using custom Kafka production exception handler with config: {}", configs);
    }

    @Override
    public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record, Exception exception) {
        logger.error("failed to produce record: {}", record, exception);
        return super.handle(record, exception);
    }
}
