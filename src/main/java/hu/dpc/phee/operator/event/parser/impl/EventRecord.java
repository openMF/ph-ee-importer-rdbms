package hu.dpc.phee.operator.event.parser.impl;

import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operator.importer.JsonPathReader;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

@RequiredArgsConstructor
@Getter
@Slf4j
public class EventRecord {

    private final Long id;
    private final String bpmnProcessId;
    private final String tenant;
    private final Long processInstanceKey;
    private final Long processDefinitionKey;
    private final String valueType;
    private final Long timestamp;
    private final String bpmnElementType;
    private final String bpmnEventType;
    private final String elementId;
    private final DocumentContext document;

    public <T> T readProperty(String name) {
        return readProperty(document, name);
    }

    public <T> T readProperty(String name, Class<T> type) {
        return readProperty(document, name, type);
    }

    public String jsonString() {
        return document.jsonString();
    }

    @Builder(builderMethodName = "listBuilder", builderClassName = "ListBuilder")
    public static List<EventRecord> createList(List<String> jsonEvents) {
        BpmnAndTenant bpmnAndTenant = null;
        Map<Long, DocumentContext> sortedRecords = new TreeMap<>(Comparator.naturalOrder());
        for (String jsonEvent : jsonEvents) {
            DocumentContext document = JsonPathReader.parse(jsonEvent);
            if (isEventMessage(document) || isEventExpired(document)) {
                continue;
            }
            if (bpmnAndTenant == null) {
                bpmnAndTenant = findBpmnAndTenant(document);
            }
            sortedRecords.put(readProperty(document, "$.position", Long.class), document);
        }
        if (bpmnAndTenant == null) {
            log.error("could not find bpmn and tenant in records:\n{}", String.join("\n", jsonEvents));
            throw new RuntimeException("could not find bpmn and tenant in records");
        }
        final String bpmn = bpmnAndTenant.bpmn();
        final String tenant = bpmnAndTenant.tenant();
        return sortedRecords.entrySet().stream()
                .map(entry -> EventRecord.builder()
                        .id(entry.getKey())
                        .document(entry.getValue())
                        .bpmn(bpmn)
                        .tenant(tenant)
                        .build())
                .toList();
    }

    @Builder
    private static EventRecord create(Long id, DocumentContext document, String bpmn, String tenant) {
        return new EventRecord(id,
                bpmn,
                tenant,
                readProperty(document, "$.value.processInstanceKey"),
                readProperty(document, "$.value.processDefinitionKey"),
                readProperty(document, "$.valueType"),
                readProperty(document, "$.timestamp"),
                readProperty(document, "$.value.bpmnElementType"),
                readProperty(document, "$.value.bpmnEventType"),
                readProperty(document, "$.value.elementId"),
                document);
    }

    private record BpmnAndTenant(String bpmn, String tenant) {
    }

    private static BpmnAndTenant findBpmnAndTenant(DocumentContext document) {
        String bpmnProcessId = getBpmnProcessId(document);
        if (StringUtils.isBlank(bpmnProcessId)) {
            return null;
        }
        String[] bpmnAndTenant = bpmnProcessId.split("-");
        if (bpmnAndTenant.length != 2) {
            return null;
        }
        String bpmn = bpmnAndTenant[0];
        String tenant = bpmnAndTenant[1];
        if (StringUtils.isNotBlank(bpmn) && StringUtils.isNotBlank(tenant)) {
            return new BpmnAndTenant(bpmn, tenant);
        }
        return null;
    }

    private static String getBpmnProcessId(DocumentContext document) {
        String bpmnProcessId = readProperty(document, "$.value.bpmnProcessId");
        if (StringUtils.isNotBlank(bpmnProcessId)) {
            return bpmnProcessId;
        }
        @SuppressWarnings("unchecked") List<String> ids = ((List<String>) readProperty(document, "$.value..bpmnProcessId")).stream().filter(Objects::nonNull).toList();
        if (ids.isEmpty()) {
            return null;
        }
        return ids.get(0);
    }

    private static boolean isEventMessage(DocumentContext document) {
        return "MESSAGE".equals(readProperty(document, "$.valueType"));
    }

    private static boolean isEventExpired(DocumentContext document) {
        return "EXPIRED".equals(readProperty(document, "$.intent"));
    }

    private static <T> T readProperty(DocumentContext document, String name) {
        return document.read(name);
    }

    private static <T> T readProperty(DocumentContext document, String name, Class<T> type) {
        return document.read(name, type);
    }
}