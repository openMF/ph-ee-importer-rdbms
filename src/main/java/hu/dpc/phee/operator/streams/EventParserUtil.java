package hu.dpc.phee.operator.streams;

import com.jayway.jsonpath.DocumentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.util.Pair;
import org.springframework.util.ObjectUtils;

import java.util.List;
import java.util.Objects;

public class EventParserUtil {

    private static Logger logger = LoggerFactory.getLogger(EventParserUtil.class);

    public static Pair<String, String> retrieveTenant(DocumentContext record) {
        String bpmnProcessIdWithTenant = findBpmnProcessId(record);
        if (ObjectUtils.isEmpty(bpmnProcessIdWithTenant)) {
            throw new RuntimeException("can't find bpmnProcessId in record: " + record.jsonString());
        }

        String[] split = bpmnProcessIdWithTenant.split("-");
        if (split.length < 2) {
            throw new RuntimeException("Invalid bpmnProcessId, has no tenant information: '" + bpmnProcessIdWithTenant + "' in record: " + record.jsonString());
        }
        return Pair.of(split[0], split[1]);
    }

    private static String findBpmnProcessId(DocumentContext record) {
        String bpmnProcessIdWithTenant = record.read("$.value.bpmnProcessId", String.class);
        if (bpmnProcessIdWithTenant == null) {
            logger.warn("can't find bpmnProcessId in record: {}, trying alternative ways..", record.jsonString());
            List<String> ids = record.read("$.value..bpmnProcessId", List.class);
            ids = ids.stream().filter(Objects::nonNull).toList();
            if (ids.size() != 1) {
                throw new RuntimeException("Invalid bpmnProcessIdWithTenant, has " + ids.size() + " bpmnProcessIds: '" + ids + "' in record: " + record.jsonString());
            }
            bpmnProcessIdWithTenant = ids.get(0);
        }
        logger.debug("resolved bpmnProcessIdWithTenant: {}", bpmnProcessIdWithTenant);
        return bpmnProcessIdWithTenant;
    }
}