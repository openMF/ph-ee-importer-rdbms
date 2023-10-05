package hu.dpc.phee.operator.streams;

import com.jayway.jsonpath.DocumentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.util.Pair;

import java.util.List;

public class EventParser {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

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
}
