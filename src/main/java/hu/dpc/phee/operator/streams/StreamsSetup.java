package hu.dpc.phee.operator.streams;

import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operator.config.TransferTransformerConfig;
import hu.dpc.phee.operator.entity.ancillary.TimestampRepository;
import hu.dpc.phee.operator.entity.ancillary.Timestamps;
import hu.dpc.phee.operator.entity.batch.BatchRepository;
import hu.dpc.phee.operator.entity.outboundmessages.OutboundMessagesRepository;
import hu.dpc.phee.operator.entity.task.Task;
import hu.dpc.phee.operator.entity.task.TaskRepository;
import hu.dpc.phee.operator.entity.tenant.ThreadLocalContextUtil;
import hu.dpc.phee.operator.entity.transactionrequest.TransactionRequestRepository;
import hu.dpc.phee.operator.entity.transfer.TransferRepository;
import hu.dpc.phee.operator.entity.variable.Variable;
import hu.dpc.phee.operator.entity.variable.VariableRepository;
import hu.dpc.phee.operator.importer.JsonPathReader;
import hu.dpc.phee.operator.tenants.TenantsService;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.common.serialization.Serdes.ListSerde;

@Service
public class StreamsSetup {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Value("${importer.kafka.topic}")
    private String kafkaTopic;

    @Value("${importer.kafka.aggreation-window-seconds}")
    private int aggregationWindowSeconds;

    @Autowired
    private StreamsBuilder streamsBuilder;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    TransferTransformerConfig transferTransformerConfig;

    @Autowired
    TransferRepository transferRepository;

    @Autowired
    TransactionRequestRepository transactionRequestRepository;

    @Autowired
    BatchRepository batchRepository;

    @Autowired
    OutboundMessagesRepository outboundMessagesRepository;

    @Autowired
    TenantsService tenantsService;

    @Autowired
    RecordParser recordParser;

    @Autowired
    VariableRepository variableRepository;

    @Autowired
    TaskRepository taskRepository;

    @Autowired
    private TimestampRepository timestampRepository;


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

        String bpmn;
        String tenantName;
        String first = records.get(0);

        DocumentContext sample = JsonPathReader.parse(first);
        try {
            Pair<String, String> bpmnAndTenant = retrieveTenant(sample);
            bpmn = bpmnAndTenant.getFirst();
            tenantName = bpmnAndTenant.getSecond();
            logger.trace("resolving tenant server connection for tenant: {}", tenantName);
            DataSource tenant = tenantsService.getTenantDataSource(tenantName);
            ThreadLocalContextUtil.setTenant(tenant);
            ThreadLocalContextUtil.setTenantName(tenantName);
        } catch (Exception e) {
            logger.error("failed to process first record: {}, skipping whole batch", first, e);
            return;
        }

        try {
            if (transferTransformerConfig.findFlow(bpmn).isEmpty()) {
                logger.warn("skip saving flow information, no configured flow found for bpmn: {}", bpmn);
                return;
            }
            Optional<TransferTransformerConfig.Flow> config = transferTransformerConfig.findFlow(bpmn);
            String flowType = getTypeForFlow(config);

            logger.info("processing key: {}, records: {}", key, records);

            transactionTemplate.executeWithoutResult(status -> {
                for (String record : records) {
                    try {
                        DocumentContext recordDocument = JsonPathReader.parse(record);
                        logToTimestampsTable(recordDocument);
                        logger.info("from kafka: {}", recordDocument.jsonString());

                        String valueType = recordDocument.read("$.valueType", String.class);
                        logger.info("processing {} event", valueType);

                        Long workflowKey = recordDocument.read("$.value.processDefinitionKey");
                        Long workflowInstanceKey = recordDocument.read("$.value.processInstanceKey");
                        Long timestamp = recordDocument.read("$.timestamp");
                        String bpmnElementType = recordDocument.read("$.value.bpmnElementType");
                        String elementId = recordDocument.read("$.value.elementId");
                        logger.info("Processing document of type {}", valueType);

                        List<Object> entities = switch (valueType) {
                            case "DEPLOYMENT", "VARIABLE_DOCUMENT", "WORKFLOW_INSTANCE" -> List.of();
                            case "PROCESS_INSTANCE" -> {
                                yield recordParser.processWorkflowInstance(recordDocument, bpmn, workflowInstanceKey, timestamp, bpmnElementType, elementId, flowType, sample);
                            }

                            case "JOB" -> {
                                yield recordParser.processTask(recordDocument, workflowInstanceKey, valueType, workflowKey, timestamp);
                            }

                            case "VARIABLE" -> {
                                yield recordParser.processVariable(recordDocument, bpmn, workflowInstanceKey, workflowKey, timestamp, flowType, sample);
                            }

                            case "INCIDENT" -> {
                                yield recordParser.processIncident(timestamp, flowType, bpmn, sample, workflowInstanceKey);
                            }

                            default -> throw new IllegalStateException("Unexpected event type: " + valueType);
                        };
                        if (entities.size() != 0) {
                            logger.info("Saving {} entities", entities.size());
                            entities.forEach(entity -> {
                                if (entity instanceof Variable) {
                                    variableRepository.save((Variable) entity);
                                } else if (entity instanceof Task) {
                                    taskRepository.save((Task) entity);
                                } else {
                                    throw new IllegalStateException("Unexpected entity type: " + entity.getClass());
                                }
                            });
                        }
                    } catch (Exception e) {
                        logger.error("failed to parse record: {}", record, e);
                    }
                }
            });
        } catch (Exception e) {
            logger.error("failed to process batch", e);

        } finally {
            ThreadLocalContextUtil.clear();
        }
    }

    private String getTypeForFlow(Optional<TransferTransformerConfig.Flow> config) {
        return config.map(TransferTransformerConfig.Flow::getType).orElse(null);
    }

    public Pair<String, String> retrieveTenant(DocumentContext record) {
        String bpmnProcessIdWithTenant = findBpmnProcessId(record);

        String[] split = bpmnProcessIdWithTenant.split("-");
        if (split.length < 2) {
            throw new RuntimeException("Invalid bpmnProcessId, has no tenant information: '" + bpmnProcessIdWithTenant + "'");
        }
        return Pair.of(split[0], split[1]);
    }

    private String findBpmnProcessId(DocumentContext record) {
        String bpmnProcessIdWithTenant = record.read("$.value.bpmnProcessId", String.class);
        if (bpmnProcessIdWithTenant == null) {
            logger.warn("can't find bpmnProcessId in record: {}, trying alternative ways..", record.jsonString());
            List<String> ids = record.read("$.value..bpmnProcessId", List.class);
            if (ids.size() > 1) {
                throw new RuntimeException("Invalid bpmnProcessIdWithTenant, has more than one bpmnProcessIds: '" + ids + "'");
            }
            bpmnProcessIdWithTenant = ids.get(0);
        }
        logger.debug("resolved bpmnProcessIdWithTenant: {}", bpmnProcessIdWithTenant);
        return bpmnProcessIdWithTenant;
    }

    public void logToTimestampsTable(DocumentContext incomingRecord) {
        try{
            Timestamps timestamps = new Timestamps();
            timestamps.setWorkflowInstanceKey(incomingRecord.read("$.value.processInstanceKey"));
            timestamps.setExportedTime(incomingRecord.read("$.exportedTime"));
            timestamps.setImportedTime(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ").format(new Date()));
            timestamps.setZeebeTime(incomingRecord.read("$.timestamp"));
            timestampRepository.save(timestamps);
        }catch (Exception e) {
            logger.info("Error parsing record");
        }
    }
}
