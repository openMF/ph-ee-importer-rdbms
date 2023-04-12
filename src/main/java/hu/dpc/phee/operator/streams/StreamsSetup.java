package hu.dpc.phee.operator.streams;

import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operator.entity.tenant.TenantServerConnection;
import hu.dpc.phee.operator.entity.tenant.TenantServerConnectionRepository;
import hu.dpc.phee.operator.entity.tenant.ThreadLocalContextUtil;
import hu.dpc.phee.operator.entity.transfer.Transfer;
import hu.dpc.phee.operator.importer.JsonPathReader;
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
import org.springframework.transaction.support.TransactionTemplate;

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

    @Autowired
    private TenantServerConnectionRepository tenantServerConnectionRepository;

    @Autowired
    private TransactionTemplate transactionTemplate;


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

    public void process(Object _key, Object _value) {
        Windowed<String> key = (Windowed<String>) _key;
        List<String> records = (List<String>) _value;

        if (records == null || records.size() == 0) {
            logger.warn("skipping processing, null records for key: {}", key);
            return;
        }

        String tenantName;
        String first = records.get(0);
        DocumentContext sample = JsonPathReader.parse(first);
        try {
            tenantName = eventParser.retrieveTenant(sample);
            logger.debug("finding tenant server connection for tenant: {}", tenantName);
            TenantServerConnection tenant = tenantServerConnectionRepository.findOneBySchemaName(tenantName);
            logger.info("setting tenant: {}", tenant);
            ThreadLocalContextUtil.setTenant(tenant);
        } catch (Exception e) {
            logger.error("failed to process first record: {}, skipping whole batch", first, e);
            return;
        }

        transactionTemplate.executeWithoutResult(status -> {
            Transfer transfer = eventParser.retrieveOrCreateTransfer(sample);

            logger.info("processing key: {}, records: {}", key, records);
            for (String record : records) {
                try {
                    eventParser.process(tenantName, transfer, record);
                } catch (Exception e) {
                    logger.error("failed to parse record: {}", record, e);
                }
            }
        });

        ThreadLocalContextUtil.clear();
    }
}
