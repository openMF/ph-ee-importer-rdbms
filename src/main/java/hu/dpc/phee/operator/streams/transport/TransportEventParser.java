package hu.dpc.phee.operator.streams.transport;

import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operator.entity.filetransport.FileTransport;
import hu.dpc.phee.operator.entity.filetransport.FileTransportRepository;
import hu.dpc.phee.operator.entity.task.TaskRepository;
import hu.dpc.phee.operator.entity.tenant.ThreadLocalContextUtil;
import hu.dpc.phee.operator.entity.variable.VariableRepository;
import hu.dpc.phee.operator.streams.EventParser;
import hu.dpc.phee.operator.streams.EventParserUtil;
import hu.dpc.phee.operator.streams.transport.config.FileTransportTransformerConfig;
import hu.dpc.phee.operator.tenants.TenantsService;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.util.Collection;

@Component
public class TransportEventParser implements EventParser {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

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

    public void process(String bpmn, String tenantName, FileTransport transport, DocumentContext record) {
        logger.info("from kafka: {}", record.jsonString());
    }

    @Override
    public boolean isAbleToProcess(Collection<DocumentContext> parsedRecords) {
        for (DocumentContext record : parsedRecords) {
            try {
                Pair<String, String> bpmnAndTenant = EventParserUtil.retrieveTenant(record);
                String bpmn = bpmnAndTenant.getFirst();
                if (fileTransportTransformerConfig.findFlow(bpmn).isPresent()) {
                    return true;
                }
            } catch (Exception e) {
                logger.trace("could not resolve bpmn name from record: {}", record);
            }
        }
        return false;
    }

    @Override
    public void process(Collection<DocumentContext> parsedRecords) {
        try {
            String bpmn = null;
            String tenantName = null;
            DocumentContext sample = null;
            for (DocumentContext record : parsedRecords) {
                sample = record;
                try {
                    Pair<String, String> bpmnAndTenant = EventParserUtil.retrieveTenant(record);
                    bpmn = bpmnAndTenant.getFirst();
                    tenantName = bpmnAndTenant.getSecond();
                    DataSource tenant = tenantsService.getTenantDataSource(tenantName);
                    ThreadLocalContextUtil.setTenant(tenant);
                    break;
                } catch (Exception e) {
                    logger.trace("could not resolve bpmn name from record: {}", record);
                }
            }

            Validate.notNull(bpmn, "could not resolve bpmn name");
            Validate.notNull(tenantName, "could not resolve tenant name");
            Validate.notNull(sample, "could not resolve sample");

            final String _bpmn = bpmn;
            final String _tenantName = tenantName;
            final DocumentContext _sample = sample;
            transactionTemplate.executeWithoutResult(status -> {
                doProcessRecords(parsedRecords, _bpmn, _sample, _tenantName);
            });
        } catch (Exception e) {
            logger.error("failed to process batch", e);
        } finally {
            ThreadLocalContextUtil.clear();
        }
    }

    private void doProcessRecords(Collection<DocumentContext> parsedRecords, String bpmn, DocumentContext sample, String tenantName) {
        FileTransport fileTransport = retrieveOrCreateFileTransport(bpmn, sample);
        try {
            MDC.put("transportId", String.valueOf(fileTransport.getWorkflowInstanceKey()));
            for (DocumentContext record : parsedRecords) {
                try {
                    process(bpmn, tenantName, fileTransport, record);
                } catch (Exception e) {
                    logger.error("failed to process record: {}", record, e);
                }
            }
            fileTransportRepository.save(fileTransport);
        } finally {
            MDC.clear();
        }
    }

    public FileTransport retrieveOrCreateFileTransport(String bpmn, DocumentContext record) {
        Long processInstanceKey = record.read("$.value.processInstanceKey", Long.class);
        FileTransport fileTransport = fileTransportRepository.findByWorkflowInstanceKey(processInstanceKey);
        if (fileTransport == null) {
            logger.debug("creating new FileTransport for processInstanceKey: {}", processInstanceKey);
            fileTransport = new FileTransport(processInstanceKey);
            fileTransportRepository.save(fileTransport);
        } else {
            logger.debug("found existing Transfer for processInstanceKey: {}", processInstanceKey);
        }
        return fileTransport;
    }
}