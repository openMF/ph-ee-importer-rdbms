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
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.util.Pair;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
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

    @Value("${importer.kafka.aggreation-window-seconds}")
    private int aggregationWindowSeconds;

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

        if (records == null || records.isEmpty()) {
            logger.warn("skipping processing, null records for key: {}", key);
            return;
        }

        String firstValid = findFirstNonExpiredRecord(records);
        if (firstValid.isEmpty()) {
            logger.debug("skipping processing, all records are expired for key: {}", key);
            return;
        }

        String bpmn;
        String tenantName;
        DocumentContext sample = JsonPathReader.parse(firstValid);
        try {
            Pair<String, String> bpmnAndTenant = eventParser.retrieveTenant(sample);
            bpmn = bpmnAndTenant.getFirst();
            tenantName = bpmnAndTenant.getSecond();
            logger.trace("resolving tenant server connection for tenant: {}", tenantName);
            DataSource tenant = tenantsService.getTenantDataSource(tenantName);
            ThreadLocalContextUtil.setTenant(tenant);
        } catch (Exception e) {
            logger.warn("could not resolve tenantName from first valid record: {}, skipping whole batch", firstValid);
            return;
        }

        try {
            if (transferTransformerConfig.findFlow(bpmn).isEmpty()) {
                logger.warn("skip saving flow information, no configured flow found for bpmn: {}", bpmn);
                return;
            }

            transactionTemplate.executeWithoutResult(status -> {
                Transfer transfer = eventParser.retrieveOrCreateTransfer(bpmn, sample);
                MDC.put("transactionId", transfer.getTransactionId());
                logger.info("processing key: {}, records: {}", key, records);

                Map<Long, DocumentContext> sortedRecords = getSortedRecords(records);
                try {
                    for (Map.Entry<Long, DocumentContext> entry : sortedRecords.entrySet()) {
                        try {
                            eventParser.process(bpmn, tenantName, transfer, entry.getValue());
                        } catch (Exception e) {
                            logger.error("failed to parse record: {}", entry.getValue(), e);
                        }
                    }

                    eventParser.save(transfer);
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


    private static @NotNull Map<Long, DocumentContext> getSortedRecords(List<String> records) {
        Map<Long, DocumentContext> sortedRecords = new TreeMap<>(Comparator.naturalOrder());
        for (String record : records) {
            DocumentContext json = JsonPathReader.parse(record);
            sortedRecords.put(json.read("$.position", Long.class), json);
        }
        return sortedRecords;
    }

    private String findFirstNonExpiredRecord(List<String> records) {
        String firstValid = "";

        for (String record : records) {
            DocumentContext sample = JsonPathReader.parse(record);
            if ("EXPIRED".equals(sample.read("$.intent"))) {
                logger.debug("skipping expired record: {}", record);
                continue;
            }
            firstValid = record;
            break;
        }
        return firstValid;
    }
}
