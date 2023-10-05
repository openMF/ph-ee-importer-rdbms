package hu.dpc.phee.operator.importer;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import org.junit.jupiter.api.Test;

import java.util.List;

public class FindBpmnProcessIdTest {
    String json = "{\n" +
            "  \"partitionId\": 1,\n" +
            "  \"value\": {\n" +
            "    \"resources\": [\n" +
            "      {\n" +
            "        \"resource\": \"cz4K\",\n" +
            "        \"resourceName\": \"ig2-incoming-recall.bpmn\"\n" +
            "      }\n" +
            "    ],\n" +
            "    \"processesMetadata\": [\n" +
            "      {\n" +
            "        \"version\": 17,\n" +
            "        \"checksum\": \"jtjo2yfFuJ94yDTFzFH5RA==\",\n" +
            "        \"bpmnProcessId\": \"binx_ig2_incoming_recall-binx\",\n" +
            "        \"duplicate\": false,\n" +
            "        \"processDefinitionKey\": 2251799837455793,\n" +
            "        \"resourceName\": \"ig2-incoming-recall.bpmn\"\n" +
            "      }\n" +
            "    ],\n" +
            "    \"decisionsMetadata\": [],\n" +
            "    \"decisionRequirementsMetadata\": []\n" +
            "  },\n" +
            "  \"key\": 2251799837455794,\n" +
            "  \"timestamp\": 1687446188134,\n" +
            "  \"position\": 47500976,\n" +
            "  \"valueType\": \"DEPLOYMENT\",\n" +
            "  \"brokerVersion\": \"8.2.2\",\n" +
            "  \"recordType\": \"EVENT\",\n" +
            "  \"sourceRecordPosition\": 47500974,\n" +
            "  \"intent\": \"CREATED\",\n" +
            "  \"rejectionType\": \"NULL_VAL\",\n" +
            "  \"rejectionReason\": \"\"\n" +
            "}";

    @Test
    public void test() {
        DocumentContext record = JsonPath.parse(json);
        List<String> bpmnProcessId = record.read("$.value..bpmnProcessId", List.class);
        System.out.println(bpmnProcessId.get(0));
    }
}
