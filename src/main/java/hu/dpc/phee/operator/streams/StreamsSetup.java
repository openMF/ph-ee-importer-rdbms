package hu.dpc.phee.operator.streams;

import hu.dpc.phee.operator.entity.transfer.Transfer;
import hu.dpc.phee.operator.importer.KafkaConsumer;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.common.serialization.Serdes.ListSerde;

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

    @Autowired
    private EventParser eventParser;


    @PostConstruct
    public void setup() {
        logger.info("## setting up kafka streams on topic `{}`, aggregating every {} seconds", kafkaTopic, aggregationWindowSeconds);
        Aggregator<String, String, List<String>> aggregator = (key, value, aggregate) -> {
            aggregate.add(value);
            return aggregate;
        };
        Merger<String, List<String>> merger = (key, first, second) -> Stream.of(first, second)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        streamsBuilder.stream(kafkaTopic, Consumed.with(STRING_SERDE, STRING_SERDE))
                .groupByKey()
                .windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ofSeconds(aggregationWindowSeconds), Duration.ZERO))
                .aggregate(ArrayList::new, aggregator, merger, Materialized.with(STRING_SERDE, ListSerde(ArrayList.class, STRING_SERDE)))
                .toStream()
                .foreach(this::process);

        // TODO kafka-ba kell leirni a vegen az entitaslistat, nem DB-be, hogy konzisztens es ujrajatszhato legyen !!
    }

    @Transactional
    public void process(Object _key, Object _value) {
        logger.warn("transaction active: {}", TransactionSynchronizationManager.isActualTransactionActive());
        Windowed<String> key = (Windowed<String>) _key;
        List<String> records = (List<String>) _value;

        if (records == null || records.size() == 0) {
            logger.warn("skipping processing, null records for key: {}", key);
            return;
        }

        String first = records.get(0);
        Transfer transfer = eventParser.retrieveOrCreateTransfer(first);

        logger.info("processing key: {}, records: {}", key, records);
        for (String record : records) {
            try {
                eventParser.process(transfer, record);
            } catch (Exception e) {
                logger.error("failed to parse record: {}", record, e);
            }
        }
    }
}
