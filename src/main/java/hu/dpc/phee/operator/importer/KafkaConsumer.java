package hu.dpc.phee.operator.importer;

import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operator.entity.tenant.TenantServerConnection;
import hu.dpc.phee.operator.entity.tenant.TenantServerConnectionRepository;
import hu.dpc.phee.operator.entity.tenant.ThreadLocalContextUtil;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class KafkaConsumer implements ConsumerSeekAware {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Value("${importer.kafka.topic}")
    private String kafkaTopic;

    @Value("${importer.kafka.reset}")
    private boolean reset;

    @Autowired
    private RecordParser recordParser;

    @Autowired
    private TenantServerConnectionRepository repository;

    @Autowired
    private TempDocumentStore tempDocumentStore;


    public void process(String rawData, String s) {
        try {
            DocumentContext incomingRecord = JsonPathReader.parse(rawData);
            logger.debug("from kafka: {}", incomingRecord.jsonString());
            if ("DEPLOYMENT".equals(incomingRecord.read("$.valueType"))) {
                logger.info("Deployment event arrived for bpmn: {}, skip processing", incomingRecord.read("$.value.deployedWorkflows[0].bpmnProcessId", String.class));
                return;
            }

            if (incomingRecord.read("$.valueType").equals("VARIABLE_DOCUMENT")) {
                logger.info("Skipping VARIABLE_DOCUMENT record ");
                return;
            }

            Long workflowKey = incomingRecord.read("$.value.processDefinitionKey");
            String bpmnprocessIdWithTenant = incomingRecord.read("$.value.bpmnProcessId");
            Long recordKey = incomingRecord.read("$.key");
            logger.info("bpmnprocessIdWithTenant: " + bpmnprocessIdWithTenant);
            if (bpmnprocessIdWithTenant == null) {
                bpmnprocessIdWithTenant = tempDocumentStore.getBpmnprocessId(workflowKey);
                if (bpmnprocessIdWithTenant == null) {
                    tempDocumentStore.storeDocument(workflowKey, incomingRecord);
                    logger.info("Record with key {} workflowkey {} has no associated bpmn, stored temporarily", recordKey, workflowKey);
                    return;
                }
            } else {
                tempDocumentStore.setBpmnprocessId(workflowKey, bpmnprocessIdWithTenant);
            }

            String tenantName = bpmnprocessIdWithTenant.substring(bpmnprocessIdWithTenant.indexOf("-") + 1);
            String bpmnprocessId = bpmnprocessIdWithTenant.substring(0, bpmnprocessIdWithTenant.indexOf("-"));
            TenantServerConnection tenant = repository.findOneBySchemaName(tenantName);
            if (tenant == null) {
                throw new RuntimeException("Tenant not found in database: '" + tenantName + "'");
            }

            logger.info("TenantServerConnection: {}", tenant);
            ThreadLocalContextUtil.setTenant(tenant);
            List<DocumentContext> documents = new ArrayList<>();
            List<DocumentContext> storedDocuments = tempDocumentStore.takeStoredDocuments(workflowKey);
            if (!storedDocuments.isEmpty()) {
                logger.info("Reprocessing {} previously stored records with workflowKey {}", storedDocuments.size(), workflowKey);
                documents.addAll(storedDocuments);
            }
            documents.add(incomingRecord);

            logger.debug("Start processing of {} documents", documents.size());
            for (DocumentContext doc : documents) {
                String valueType = null;
                try {
                    valueType = doc.read("$.valueType");
                    logger.info("Processing document of type {}", valueType);
                    switch (valueType) {
                        case "VARIABLE" -> {
                            DocumentContext processedVariable = recordParser.processVariable(doc); // TODO prepare for parent workflow
                            recordParser.addVariableToEntity(processedVariable, bpmnprocessId); // Call to store transfer
                        }
                        case "JOB" -> recordParser.processTask(doc);
                        case "PROCESS_INSTANCE" -> {
                            if ("PROCESS".equals(doc.read("$.value.bpmnElementType"))) {
                                recordParser.processWorkflowInstance(doc);
                            }
                        }
                        case "INCIDENT" -> logger.info("Doc {}", incomingRecord.jsonString());
                    }
                } catch (Exception ex) {
                    logger.error("Failed to process document:\n{}\nof valueType:\n{}\nerror: {}\ntrace: {}",
                            doc.jsonString(),
                            valueType,
                            ex.getMessage(),
                            limitStackTrace(ex));
                    tempDocumentStore.storeDocument(workflowKey, doc);
                }
            }
        } catch (Exception ex) {
            logger.error("Could not parse zeebe event:\n{}\nerror: {}\ntrace: {}", rawData, ex.getMessage(), s);
        } finally {
            ThreadLocalContextUtil.clear();
        }
    }

    private String limitStackTrace(Exception ex) {
        return Arrays.stream(ex.getStackTrace())
                .limit(10)
                .map(StackTraceElement::toString)
                .collect(Collectors.joining("\n"));
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        if (reset) {
            assignments.keySet().stream()
                    .filter(partition -> partition.topic().equals(kafkaTopic))
                    .forEach(partition -> {
                        callback.seekToBeginning(partition.topic(), partition.partition());
                        logger.info("seeked {} to beginning", partition);
                    });
        } else {
            logger.info("no reset, consuming kafka topics from latest");
        }
    }
}
