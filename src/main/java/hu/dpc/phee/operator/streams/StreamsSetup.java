package hu.dpc.phee.operator.streams;

import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operator.importer.JsonPathReader;
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

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.common.serialization.Serdes.ListSerde;

@Service
public class StreamsSetup {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Value("${importer.kafka.topic}")
    private String kafkaTopic;

    @Value("${importer.kafka.aggregation-window-seconds}")
    private int aggregationWindowSeconds;

    @Value("${importer.kafka.aggregation-after-end-seconds}")
    private int aggregationAfterEndSeconds;

    @Autowired
    private StreamsBuilder streamsBuilder;

    @Autowired
    private List<EventParser> parsers;

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
                .windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ofSeconds(aggregationWindowSeconds), Duration.ofSeconds(aggregationAfterEndSeconds)))
                .aggregate(ArrayList::new, aggregator, merger, Materialized.with(STRING_SERDE, ListSerde(ArrayList.class, STRING_SERDE)))
                .toStream()
                .foreach(this::process);
    }

    public void process(Object _key, Object _value) {
        String key = ((Windowed<String>) _key).key();
        List<String> records = (List<String>) _value;

        if (records == null || records.isEmpty()) {
            logger.warn("skipping processing, received no records for key: {}", key);
            return;
        }

        Collection<DocumentContext> parsedRecords = parseAndSortInterestingRecords(records);
        if (parsedRecords.isEmpty()) {
            logger.debug("skipping processing, all records are messages or expired for key: {}", key);
            return;
        }

        logger.info("received {} records for key: {}", parsedRecords.size(), key);

        EventParser parser = parsers.stream()
                .filter(p -> p.isAbleToProcess(parsedRecords))
                .findFirst().orElseThrow();

        parser.process(parsedRecords);
    }

    private Collection<DocumentContext> parseAndSortInterestingRecords(List<String> records) {
        Map<Long, DocumentContext> sortedRecords = new TreeMap<>(Comparator.naturalOrder());
        for (String record : records) {
            DocumentContext json = JsonPathReader.parse(record);
            if (isNotMessageType(json) && isNotExpired(json)) {
                sortedRecords.put(json.read("$.position", Long.class), json);
            }
        }
        return sortedRecords.values();
    }

    private boolean isNotMessageType(DocumentContext json) {
        return !"MESSAGE".equals(json.read("$.valueType"));
    }

    private boolean isNotExpired(DocumentContext json) {
        return !"EXPIRED".equals(json.read("$.intent"));
    }
}