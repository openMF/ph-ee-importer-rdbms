package hu.dpc.phee.operator.streams;

import hu.dpc.phee.operator.streams.impl.EventRecord;
import hu.dpc.phee.operator.streams.impl.config.ImporterConfig;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.common.serialization.Serdes.ListSerde;

@Service
@Slf4j
public class StreamsSetup {

    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Autowired
    private ImporterConfig config;

    @Autowired
    private StreamsBuilder streamsBuilder;

    @Autowired
    private List<EventParser> parsers;

    @PostConstruct
    public void setup() {
        log.info("setting up kafka streams on topic {}, aggregating every {} seconds", config.kafka().topic(), config.kafka().aggregationWindowSeconds());

        Aggregator<String, String, List<String>> aggregator = (key, value, aggregate) -> {
            aggregate.add(value);
            return aggregate;
        };

        Merger<String, List<String>> merger = (key, first, second) -> Stream.of(first, second)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        streamsBuilder.stream(config.kafka().topic(), Consumed.with(STRING_SERDE, STRING_SERDE))
                .groupByKey()
                .windowedBy(SessionWindows.ofInactivityGapAndGrace(
                        Duration.ofSeconds(config.kafka().aggregationWindowSeconds()),
                        Duration.ofSeconds(config.kafka().aggregationAfterEndSeconds())))
                .aggregate(ArrayList::new, aggregator, merger, Materialized.with(STRING_SERDE, ListSerde(ArrayList.class, STRING_SERDE)))
                .toStream()
                .foreach(this::process);
    }

    public void process(Object _key, Object _value) {
        @SuppressWarnings("unchecked")
        String key = ((Windowed<String>) _key).key();
        @SuppressWarnings("unchecked")
        List<String> events = (List<String>) _value;

        if (events == null || events.isEmpty()) {
            log.warn("received no records for key {}", key);
            return;
        }

        List<EventRecord> eventRecords = EventRecord.listBuilder().jsonEvents(events).build();
        if (eventRecords.isEmpty()) {
            log.debug("received no valid records for key {}", key);
            return;
        }

        log.debug("received {} records for key {}", eventRecords.size(), key);
        Optional<EventParser> validEventParser = parsers.stream()
                .filter(p -> p.isAbleToProcess(eventRecords))
                .findFirst();

        if (validEventParser.isEmpty()) {
            log.warn("found no valid parser for records {}", key);
            return;
        }

        EventParser parser = validEventParser.get();
        log.info("parser {} processing {} records with key {}", parser.getBeanName(), eventRecords.size(), key);
        parser.process(eventRecords);
    }
}