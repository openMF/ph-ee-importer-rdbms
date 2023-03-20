package hu.dpc.phee.operator.streams;

import hu.dpc.phee.operator.importer.KafkaConsumer;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class StreamsSetup {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Value("${importer.kafka.topic}")
    private String kafkaTopic;

    @Value("${importer.kafka.aggreation-window-seconds}")
    private int aggregationWindowSeconds;

    @Autowired
    private StreamsBuilder streamsBuilder;

    @Autowired
    private KafkaConsumer kafkaConsumer;


    @PostConstruct
    public void setup() {
        logger.info("## setting up kafka streams on topic `{}`, aggregating every {} seconds", kafkaTopic, aggregationWindowSeconds);
        KTable<Windowed<String>, String> reduced = streamsBuilder.stream(kafkaTopic, Consumed.with(STRING_SERDE, STRING_SERDE))
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(aggregationWindowSeconds)))
                .reduce(this::aggregate);

        reduced
                .toStream()
                .foreach(this::process);
    }

    private String aggregate(String agg, String value) {
        logger.warn("aggregating: {} + {}", agg, value);
        return agg + "|" + value;
    }

    private void process(Windowed<String> key, String value) {
        logger.info("processing key: {}, value: {}", key, value);
        try {
            kafkaConsumer.process(value);
        } catch (Exception e) {
            logger.error("PANIC on error", e);  // TODO for now, just exit
            System.exit(1);
        }
    }
}
