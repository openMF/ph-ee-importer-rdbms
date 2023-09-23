package hu.dpc.phee.operator.importer;

import com.jayway.jsonpath.*;
import hu.dpc.phee.operator.entity.task.Task;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class JsonParseTest {

    String taskCreated = "{\n" +
            "  \"partitionId\": 3,\n" +
            "  \"sourceRecordPosition\": 12885411520,\n" +
            "  \"recordType\": \"EVENT\",\n" +
            "  \"valueType\": \"WORKFLOW_INSTANCE\",\n" +
            "  \"position\": 12885411896,\n" +
            "  \"rejectionType\": \"NULL_VAL\",\n" +
            "  \"rejectionReason\": \"\",\n" +
            "  \"value\": {\n" +
            "    \"elementId\": \"Task_1lfzg19\",\n" +
            "    \"variables\": {},\n" +
            "    \"errorMessage\": \"\",\n" +
            "    \"errorCode\": \"\",\n" +
            "    \"type\": \"payee-party-lookup-DFSPID\",\n" +
            "    \"retries\": 3,\n" +
            "    \"elementInstanceKey\": 6755399441058323,\n" +
            "    \"workflowKey\": 2251799813687425,\n" +
            "    \"workflowInstanceKey\": 6755399441058311,\n" +
            "    \"bpmnProcessId\": \"PayeePartyLookup-tn01\",\n" +
            "    \"deadline\": -1,\n" +
            "    \"worker\": \"\",\n" +
            "    \"customHeaders\": {},\n" +
            "    \"workflowDefinitionVersion\": 1\n" +
            "  },\n" +
            "  \"intent\": \"CREATED\",\n" +
            "  \"key\": 6755399441058324,\n" +
            "  \"timestamp\": 1586104064972\n" +
            "}";

    String processActivating = "{\"partitionId\":1,\n" +
            "\"value\":\n" +
            "{\n" +
            "\"version\":1,\n" +
            "\"flowScopeKey\":-1,\n" +
            "\"bpmnElementType\":\"PROCESS\",\n" +
            "\"parentWorkflowInstanceKey\":-1,\n" +
            "\"parentElementInstanceKey\":-1,\n" +
            "\"workflowInstanceKey\":2251799813686963,\n" +
            "\"bpmnProcessId\":\"PayerFundTransfer-tn01\",\n" +
            "\"workflowKey\":2251799813686925,\n" +
            "\"elementId\":\"PayerFundTransfer-tn01\"\n" +
            "},\n" +
            "\"sourceRecordPosition\":4296365776,\n" +
            "\"position\":4296367480,\n" +
            "\"key\":2251799813686963,\n" +
            "\"timestamp\":1590762784476,\n" +
            "\"valueType\":\"WORKFLOW_INSTANCE\",\n" +
            "\"recordType\":\"EVENT\",\n" +
            "\"rejectionType\":\"NULL_VAL\",\n" +
            "\"rejectionReason\":\"\",\n" +
            "\"intent\":\"ELEMENT_ACTIVATING\"\n" +
            "}";

    String deployment = "{\n" +
            "\"partitionId\":1,\n" +
            "\"value\":\n" +
            "{\n" +
            "\"resources\":\n" +
            "[{\n" +
            "\"resource\":\"---------\",\n" +
            "\"resourceName\":\"./orchestration/feel/transfer-process-DFSPID.bpmn\",\n" +
            "\"resourceType\":\"BPMN_XML\"\n" +
            "}],\n" +
            "\"deployedWorkflows\":\n" +
            "[{\n" +
            "\"version\":1,\n" +
            "\"resourceName\":\"./orchestration/feel/transfer-process-DFSPID.bpmn\",\n" +
            "\"bpmnProcessId\":\"transfer-process-tn02\",\n" +
            "\"workflowKey\":2251799813688244\n" +
            "}]\n" +
            "},\n" +
            "\"sourceRecordPosition\":4299925008,\n" +
            "\"position\":4299964072,\n" +
            "\"key\":2251799813688245,\n" +
            "\"timestamp\":1591957096471,\n" +
            "\"valueType\":\"DEPLOYMENT\",\n" +
            "\"rejectionType\":\"NULL_VAL\",\n" +
            "\"rejectionReason\":\"\",\n" +
            "\"intent\":\"CREATED\",\n" +
            "\"recordType\":\"EVENT\"\n" +
            "}";

    String variableCreated = "{\n" +
            "  \"partitionId\": 3,\n" +
            "  \"sourceRecordPosition\": 12885411520,\n" +
            "  \"recordType\": \"EVENT\",\n" +
            "  \"valueType\": \"VARIABLE\",\n" +
            "  \"position\": 12885411896,\n" +
            "  \"rejectionType\": \"NULL_VAL\",\n" +
            "  \"rejectionReason\": \"\",\n" +
            "  \"value\": {\n" +
            "    \"name\": \"transactionId\",\n" +
            "    \"value\": \"abc-123-def-456\",\n" +
            "    \"workflowKey\": 2251799813687425,\n" +
            "    \"workflowInstanceKey\": 6755399441058311,\n" +
            "    \"scopeKey\": 6755399441058311,\n" +
            "  },\n" +
            "  \"intent\": \"CREATED\",\n" +
            "  \"key\": 6755399441058324,\n" +
            "  \"timestamp\": 1586104064972\n" +
            "}";

    @Test
    public void test() {
        Configuration config = Configuration.defaultConfiguration()
                .addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL)
                .addOptions(Option.SUPPRESS_EXCEPTIONS);
        ParseContext jsonParser = JsonPath.using(config);

        DocumentContext doc = jsonParser.parse(taskCreated);
        Object read = doc.read("$.value.bpmnProcessId");
        Task task = new Task();
        task.setWorkflowKey(doc.read("$.value.workflowKey"));
        task.setWorkflowInstanceKey(doc.read("$.value.workflowInstanceKey"));
        System.out.println(task);
    }

    @Test
    @Disabled
    public void testSendKafkaMessage() {
        Map<String, Object> kafkaProperties = new HashMap<>();

        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProperties.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<Object, Object> producer = new KafkaProducer<>(kafkaProperties);

        producer.send(new ProducerRecord<>("zeebe-export", "0-1", processActivating));
        producer.flush();
    }
}