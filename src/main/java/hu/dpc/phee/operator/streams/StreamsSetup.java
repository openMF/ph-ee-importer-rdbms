package hu.dpc.phee.operator.streams;

import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operator.config.TransferTransformerConfig;
import hu.dpc.phee.operator.entity.tenant.ThreadLocalContextUtil;
import hu.dpc.phee.operator.entity.transfer.Transfer;
import hu.dpc.phee.operator.entity.transfer.TransferRepository;
import hu.dpc.phee.operator.importer.JsonPathReader;
import hu.dpc.phee.operator.tenants.TenantsService;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.common.serialization.Serdes.ListSerde;
import static org.apache.kafka.streams.kstream.Materialized.as;

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
    private EventParser eventParser;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    TransferTransformerConfig transferTransformerConfig;

    @Autowired
    TransferRepository transferRepository;

    @Autowired
    TenantsService tenantsService;


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

//        Materialized<String, List<String>, SessionStore<Bytes, byte[]>> materialized = Materialized.with(STRING_SERDE, ListSerde(ArrayList.class, STRING_SERDE));
        Materialized<String, List<String>, SessionStore<Bytes, byte[]>> materialized = as(Stores.inMemorySessionStore("session-store", Duration.ofSeconds(5)));

        streamsBuilder.stream(kafkaTopic, Consumed.with(STRING_SERDE, STRING_SERDE))
                .groupByKey()
                .windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ofSeconds(aggregationWindowSeconds), Duration.ofSeconds(aggregationAfterEndSeconds)))
                .aggregate(ArrayList::new, aggregator, merger, materialized)
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

        String bpmn = null;
        String tenantName = null;
        DocumentContext sample = null;
        for (DocumentContext record : parsedRecords) {
            sample = record;
            try {
                Pair<String, String> bpmnAndTenant = eventParser.retrieveTenant(record);
                bpmn = bpmnAndTenant.getFirst();
                tenantName = bpmnAndTenant.getSecond();
                logger.trace("resolving tenant server connection for tenant: {}", tenantName);
                DataSource tenant = tenantsService.getTenantDataSource(tenantName);
                ThreadLocalContextUtil.setTenant(tenant);

                if (transferTransformerConfig.findFlow(bpmn).isEmpty()) {
                    logger.warn("skipping record, no configured flow found for bpmn: {}", bpmn);
                    continue;
                }

                break;
            } catch (Exception e) {
                logger.warn("could not resolve tenantName from record: {}", record);
            }
        }
        if (bpmn == null) {
            logger.error("could not resolve tenantName for key: {}, skipping whole batch", key);
            return;
        }

        doProcess(bpmn, sample, key, parsedRecords, tenantName);
    }

    private void doProcess(String bpmn, DocumentContext sample, String key, Collection<DocumentContext> parsedRecords, String tenantName) {
        try {
            transactionTemplate.executeWithoutResult(status -> {
                Transfer transfer = eventParser.retrieveOrCreateTransfer(bpmn, sample);
                try {
                    MDC.put("transactionId", transfer.getTransactionId());
                    for (DocumentContext record : parsedRecords) {
                        try {
                            eventParser.process(bpmn, tenantName, transfer, record);
                        } catch (Exception e) {
                            logger.error("failed to process record: {}", record, e);
                        }
                    }
                    eventParser.transferRepository.save(transfer);
                } finally {
                    MDC.clear();
                }
            });

        } catch (Exception e) {
            logger.error("failed to process batch", e);

        } finally {
            ThreadLocalContextUtil.clear();
        }
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
