package hu.dpc.phee.operator.streams.impl.transport;

import com.baasflow.commons.events.EventLogLevel;
import com.baasflow.commons.events.EventService;
import com.baasflow.commons.events.EventStatus;
import com.baasflow.commons.events.EventType;
import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operator.config.transformer.Transformer;
import hu.dpc.phee.operator.entity.filetransport.FileTransport;
import hu.dpc.phee.operator.entity.filetransport.FileTransportRepository;
import hu.dpc.phee.operator.entity.task.Task;
import hu.dpc.phee.operator.entity.task.TaskRepository;
import hu.dpc.phee.operator.entity.tenant.ThreadLocalContextUtil;
import hu.dpc.phee.operator.entity.transfer.Transfer;
import hu.dpc.phee.operator.entity.transfer.TransferStatus;
import hu.dpc.phee.operator.entity.variable.Variable;
import hu.dpc.phee.operator.entity.variable.VariableRepository;
import hu.dpc.phee.operator.importer.JsonPathReader;
import hu.dpc.phee.operator.streams.EventParser;
import hu.dpc.phee.operator.streams.impl.EventRecord;
import hu.dpc.phee.operator.streams.impl.transport.config.FileTransportTransformerConfig;
import hu.dpc.phee.operator.tenants.TenantsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.logging.log4j.util.Strings;
import org.jetbrains.annotations.NotNull;
import org.slf4j.MDC;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathFactory;
import java.io.StringReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
@Slf4j
public class TransportEventParser implements EventParser {

    @Autowired
    private VariableRepository variableRepository;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private FileTransportRepository fileTransportRepository;

    @Autowired
    private FileTransportTransformerConfig fileTransportTransformerConfig;

    @Autowired
    private TenantsService tenantsService;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    private EventService eventService;

    private final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
    private final XPathFactory xPathFactory = XPathFactory.newInstance();

    @Override
    public boolean isAbleToProcess(List<EventRecord> eventRecords) {
        return eventRecords.stream()
                .anyMatch(e -> fileTransportTransformerConfig.findFlow(e.getBpmnProcessId()).isPresent());
    }

    @Override
    public void process(List<EventRecord> eventRecords) {
        log.debug("processing {} records in TransportEventParser", eventRecords.size());
        try {
            EventRecord first = eventRecords.get(0);
            String tenantName = first.getTenant();
            Long processInstanceKey = first.getProcessInstanceKey();
            String bpmn = first.getBpmnProcessId();
            ThreadLocalContextUtil.setTenant(tenantsService.getTenantDataSource(tenantName));
            MDC.put("transactionId", String.valueOf(processInstanceKey));
            processEventRecords(eventRecords, bpmn, processInstanceKey, tenantName);
        } catch (Exception e) {
            log.error("failed to process records", e);
        } finally {
            MDC.clear();
            ThreadLocalContextUtil.clear();
        }
    }

    private void processEventRecords(List<EventRecord> eventRecords, String bpmn, Long processInstanceKey, String tenantName) {
        log.info("processing FileTransport event records for bpmn {} and processInstanceKey {} in tenant {}", bpmn, processInstanceKey, tenantName);
        transactionTemplate.executeWithoutResult(status -> {
            FileTransport fileTransport = retrieveOrCreateFileTransport(processInstanceKey);
            for (EventRecord eventRecord : eventRecords) {
                log.trace("FileTransportEventParser processing record: {}", eventRecord.jsonString());
                try {
                    processRecord(fileTransport, eventRecord);
                } catch (Exception e) {
                    log.error("failed to process record with FileTransportEventParser: {}", eventRecord, e);
                }
            }
            fileTransportRepository.save(fileTransport);
        });
    }

    private FileTransport retrieveOrCreateFileTransport(long processInstanceKey) {
        FileTransport fileTransport = fileTransportRepository.findByWorkflowInstanceKey(processInstanceKey);
        if (fileTransport != null) {
            log.debug("found existing FileTransport with id {}", processInstanceKey);
            return fileTransport;
        }
        log.debug("creating new FileTransport with id {}", processInstanceKey);
        FileTransport newFileTransport = new FileTransport(processInstanceKey);
        fileTransportRepository.save(newFileTransport);
        return newFileTransport;
    }

    private void processRecord(FileTransport transport, EventRecord eventRecord) {
        if (log.isTraceEnabled()) {
            log.trace("{} event is: {}", eventRecord.getValueType(), eventRecord.jsonString());
        } else {
            log.debug("event type is {}", eventRecord.getValueType());
        }
        switch (eventRecord.getValueType()) {
            case "PROCESS_INSTANCE" -> processInstance(transport, eventRecord);
            case "JOB" -> processJob(eventRecord);
            case "VARIABLE" -> processVariable(transport, eventRecord);
            case "INCIDENT" -> processIncident(transport, eventRecord);
            default -> log.error("unknown event type: {}", eventRecord.getValueType());
        }
        fileTransportRepository.save(transport);
    }

    private void processInstance(FileTransport transport, EventRecord eventRecord) {
        String recordType = eventRecord.readProperty("$.recordType");
        String intent = eventRecord.readProperty("$.intent");

        if ("EVENT".equals(recordType) && "START_EVENT".equals(eventRecord.getBpmnElementType()) && "ELEMENT_ACTIVATED".equals(intent)) {
            transport.setStartedAt(new Date(eventRecord.getTimestamp()));
            transport.setLastUpdated(eventRecord.getTimestamp());

            List<Transformer> constantTransformers = fileTransportTransformerConfig.getFlows().stream()
                    .filter(flow -> eventRecord.getBpmnProcessId().equalsIgnoreCase(flow.getName()))
                    .flatMap(flow -> flow.getTransformers().stream())
                    .filter(it -> Strings.isNotBlank(it.getConstant()))
                    .toList();

            log.debug("found {} constant transformers for flow start {}", constantTransformers.size(), eventRecord.getBpmnProcessId());
            constantTransformers.forEach(it -> applyTransformer(eventRecord.getTimestamp(), transport, null, null, it));
        }

        if ("EVENT".equals(recordType) && "END_EVENT".equals(eventRecord.getBpmnElementType()) && "ELEMENT_COMPLETED".equals(intent)) {
            log.info("finishing transfer for processInstanceKey: {} at elementId: {}", eventRecord.getProcessInstanceKey(), eventRecord.getElementId());
            transport.setCompletedAt(new Date(eventRecord.getTimestamp()));
            if (StringUtils.isNotEmpty(eventRecord.getElementId()) && eventRecord.getElementId().contains("Failed")) {
                transport.setStatus(FileTransport.TransportStatus.FAILED);
            } else {
                transport.setStatus(FileTransport.TransportStatus.COMPLETED);
            }
            transport.setLastUpdated(eventRecord.getTimestamp());
        }

        if ("EVENT".equals(recordType) && "EXCLUSIVE_GATEWAY".equals(eventRecord.getBpmnElementType()) && "ELEMENT_COMPLETED".equals(intent)) {
            log.info("exclusive gateway completed for processInstanceKey: {} at elementId: {}", eventRecord.getProcessInstanceKey(), eventRecord.getElementId());
            Task task =new Task()
                    .withWorkflowInstanceKey(eventRecord.getProcessInstanceKey())
                    .withWorkflowKey(eventRecord.getProcessDefinitionKey())
                    .withTimestamp(eventRecord.getTimestamp())
                    .withIntent(intent)
                    .withRecordType(recordType)
                    .withType("EXCLUSIVE_GATEWAY")
                    .withElementId(eventRecord.getElementId());
            taskRepository.save(task);
        }

        if ("EVENT".equals(recordType) && "TIMER".equals(eventRecord.getBpmnEventType()) && "ELEMENT_ACTIVATED".equals(intent)) {
            log.info("timer event for processInstanceKey: {} at elementId: {}", eventRecord.getProcessInstanceKey(), eventRecord.getElementId());
            Task task =new Task()
                    .withWorkflowInstanceKey(eventRecord.getProcessInstanceKey())
                    .withWorkflowKey(eventRecord.getProcessDefinitionKey())
                    .withTimestamp(eventRecord.getTimestamp())
                    .withIntent(intent)
                    .withRecordType(recordType)
                    .withType("TIMER")
                    .withElementId(eventRecord.getElementId());
            taskRepository.save(task);
        }

        if ("EVENT".equals(recordType) && "MESSAGE".equals(eventRecord.getBpmnEventType()) && "ELEMENT_ACTIVATED".equals(intent)) {
            log.info("message event for processInstanceKey: {} at elementId: {}", eventRecord.getProcessInstanceKey(), eventRecord.getElementId());
            Task task = new Task()
                    .withWorkflowInstanceKey(eventRecord.getProcessInstanceKey())
                    .withWorkflowKey(eventRecord.getProcessDefinitionKey())
                    .withTimestamp(eventRecord.getTimestamp())
                    .withIntent(intent)
                    .withRecordType(recordType)
                    .withType("MESSAGE")
                    .withElementId(eventRecord.getElementId());
            taskRepository.save(task);
        }
    }

    private void processVariable(FileTransport transport, EventRecord eventRecord) {
        log.debug("processing variable in flow {}", eventRecord.getBpmnProcessId());

        String variableName = eventRecord.readProperty("$.value.name");
        String variableValue = eventRecord.readProperty("$.value.value");
        String value = variableValue.startsWith("\"") && variableValue.endsWith("\"") ? StringEscapeUtils.unescapeJson(variableValue.substring(1, variableValue.length() - 1)) : variableValue;

        log.trace("{} = {}", variableName, variableValue);

        List<Transformer> matchingTransformers = fileTransportTransformerConfig.getFlows().stream()
                .filter(flow -> eventRecord.getBpmnProcessId().equalsIgnoreCase(flow.getName()))
                .flatMap(flow -> flow.getTransformers().stream())
                .filter(transformer -> variableName.equalsIgnoreCase(transformer.getVariableName()))
                .toList();

        matchingTransformers.forEach(transformer -> applyTransformer(eventRecord.getTimestamp(), transport, variableName, value, transformer));

        Variable variable = new Variable()
                .withWorkflowInstanceKey(eventRecord.getProcessInstanceKey())
                .withName(variableName)
                .withWorkflowKey(eventRecord.getProcessDefinitionKey())
                .withTimestamp(eventRecord.getTimestamp())
                .withPosition(eventRecord.getId())
                .withValue(variableValue);
        variableRepository.save(variable);
    }

    private void processIncident(FileTransport transport, EventRecord eventRecord) {
        log.warn("processing incident in flow {}", eventRecord.getBpmnProcessId());

        transport.setStatus(FileTransport.TransportStatus.EXCEPTION);
        transport.setCompletedAt(new Date(eventRecord.getTimestamp()));
        transport.setLastUpdated(eventRecord.getTimestamp());

        eventService.sendEvent(event -> event
                .setSourceModule("importer")
                .setEventLogLevel(EventLogLevel.ERROR)
                .setEventType(EventType.audit)
                .setEvent("Incident event received for flow")
                .setEventStatus(EventStatus.failure)
                .setTenantId(eventRecord.getTenant())
                .setCorrelationIds(Map.of(
                        "processInstanceId", eventRecord.getProcessInstanceKey().toString()
                ))
                .setPayload(eventRecord.jsonString())
                .setPayloadType("string")
        );

        Variable variable = new Variable()
                .withWorkflowInstanceKey(eventRecord.getProcessInstanceKey())
                .withName("exception")
                .withWorkflowKey(eventRecord.getProcessDefinitionKey())
                .withTimestamp(eventRecord.getTimestamp())
                .withPosition(eventRecord.getId())
                .withValue(StringEscapeUtils.escapeJson(eventRecord.jsonString()));
        variableRepository.save(variable);
    }

    private void processJob(@NotNull EventRecord eventRecord) {
        log.debug("processing job/task in flow {}", eventRecord.getBpmnProcessId());
        Task task = new Task()
                .withWorkflowInstanceKey(eventRecord.getProcessInstanceKey())
                .withWorkflowKey(eventRecord.getProcessDefinitionKey())
                .withTimestamp(eventRecord.getTimestamp())
                .withIntent(eventRecord.readProperty("$.intent"))
                .withRecordType(eventRecord.getValueType())
                .withType(eventRecord.readProperty("$.value.type"))
                .withElementId(eventRecord.getElementId());
        taskRepository.save(task);
    }

    private void applyTransformer(Long timestamp, FileTransport transport, String variableName, String variableValue, Transformer transformer) {
        log.debug("applying transformer for field: {}", transformer.getField());
        try {
            String fieldName = transformer.getField();
            String dateFormat = transformer.getDateFormat();
            if (Strings.isNotBlank(transformer.getConstant())) {
                log.debug("setting constant value: {}", transformer.getConstant());
                setPropertyValue(timestamp, transport, fieldName, transformer.getConstant(), dateFormat);
                return;
            }

            if (Strings.isNotBlank(transformer.getJsonPath())) {
                log.debug("applying jsonpath for variable {}", variableName);
                DocumentContext json = JsonPathReader.parse(variableValue);
                Object result = json.read(transformer.getJsonPath());
                log.debug("jsonpath result: {} for variable {}", result, variableName);

                String value = null;
                if (result != null) {
                    if (result instanceof List) {
                        value = ((List<?>) result).stream().map(Object::toString).collect(Collectors.joining(" "));
                    } else {
                        value = result.toString();
                    }
                    setPropertyValue(timestamp, transport, fieldName, value, dateFormat);
                }

                if (StringUtils.isBlank(value)) {
                    log.error("null result when setting field {} from variable {}. Jsonpath: {}, variable value: {}", fieldName, variableName, transformer.getJsonPath(), variableValue);
                }
                return;
            }

            if (Strings.isNotBlank(transformer.getXpath())) {
                log.debug("applying xpath for variable {}", variableName);
                Document document = documentBuilderFactory.newDocumentBuilder().parse(new InputSource(new StringReader(variableValue)));
                String result = xPathFactory.newXPath().compile(transformer.getXpath()).evaluate(document);
                log.debug("xpath result: {} for variable {}", result, variableName);
                if (StringUtils.isNotBlank(result)) {
                    setPropertyValue(timestamp, transport, fieldName, result, dateFormat);
                } else {
                    log.error("null result when setting field {} from variable {}. Xpath: {}, variable value: {}", fieldName, variableName, transformer.getXpath(), variableValue);
                }
                return;
            }

            log.debug("setting simple variable value: {} for variable {}", variableValue, variableName);
            setPropertyValue(timestamp, transport, fieldName, variableValue, dateFormat);

        } catch (Exception e) {
            log.error("failed to apply transformer {} to variable {}", transformer, variableName, e);
        }
    }

    private void setPropertyValue(Long timestamp, FileTransport transport, String fieldName, String variableValue, String dateFormat) {
        if (Date.class.getName().equals(PropertyAccessorFactory.forBeanPropertyAccess(transport).getPropertyType(fieldName).getName())) {
            try {
                log.debug("Parsing date {} with format {}", variableValue, dateFormat);
                PropertyAccessorFactory.forBeanPropertyAccess(transport).setPropertyValue(fieldName, new SimpleDateFormat(dateFormat).parse(variableValue));
            } catch (ParseException pe) {
                log.warn("failed to parse date {} with format {}", variableValue, dateFormat);
            }
        } else {
            PropertyAccessorFactory.forBeanPropertyAccess(transport).setPropertyValue(fieldName, variableValue);
        }
        transport.setLastUpdated(timestamp);
    }

    @Override
    public @NotNull String getBeanName() {
        return this.getClass().getName();
    }
}