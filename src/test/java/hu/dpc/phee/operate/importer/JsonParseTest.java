package hu.dpc.phee.operate.importer;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import hu.dpc.phee.operate.importer.persistence.Task;
import org.junit.Test;

public class JsonParseTest {

    String json = "{\n" +
            "  \"partitionId\": 3,\n" +
            "  \"sourceRecordPosition\": 12885411520,\n" +
            "  \"recordType\": \"EVENT\",\n" +
            "  \"valueType\": \"JOB\",\n" +
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
            "    \"bpmnProcessId\": \"PayeePartyLookup-DFSPID\",\n" +
            "    \"deadline\": -1,\n" +
            "    \"worker\": \"\",\n" +
            "    \"customHeaders\": {},\n" +
            "    \"workflowDefinitionVersion\": 1\n" +
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

        DocumentContext doc = jsonParser.parse(json);
        Task task = new Task();
        task.setWorkflowKey(doc.read("$.value.workflowKey"));
        task.setWorkflowInstanceKey(doc.read("$.value.workflowInstanceKey"));
        System.out.println(task);
    }
}