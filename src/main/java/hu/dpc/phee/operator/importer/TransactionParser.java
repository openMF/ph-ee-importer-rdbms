package hu.dpc.phee.operator.importer;

import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operator.business.Transaction;
import hu.dpc.phee.operator.business.TransactionDirection;
import hu.dpc.phee.operator.business.TransactionRepository;
import hu.dpc.phee.operator.business.TransactionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static hu.dpc.phee.operator.business.TransactionDirection.fromBpmn;

@Component
public class TransactionParser {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final String OUTGOING_BPMN_NAME = "PayerFundTransfer-DFSPID";
    private static final String INCOMING_BPMN_NAME = "PayeeQuoteTransfer-DFSPID";
    private static final List<String> TRANSFER_BPMN_NAMES = Arrays.asList(INCOMING_BPMN_NAME, OUTGOING_BPMN_NAME);

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private InflightTransactionManager inflightTransactionManager;


    private Map<String, Consumer<Pair<Transaction, String>>> VARIABLE_PARSERS = new HashMap<>();


    public TransactionParser() {
        VARIABLE_PARSERS.putAll(IncomingVariableParsers.VARIABLE_PARSERS);
        VARIABLE_PARSERS.putAll(OutgoingVariableParsers.VARIABLE_PARSERS);
    }


    public void processVariable(DocumentContext json) {
        Long workflowInstanceKey = json.read("$.value.workflowInstanceKey");

        String name = json.read("$.value.name");

        if (VARIABLE_PARSERS.containsKey(name)) {
            logger.debug("parsing variable {}", name);
            String value = json.read("$.value.value");

            Transaction transaction = inflightTransactionManager.getOrCreateTransaction(workflowInstanceKey);
            VARIABLE_PARSERS.get(name).accept(Pair.of(transaction, value));
            if (transaction.getStartedAt() != null) {
                transactionRepository.save(transaction);
            } else {
                logger.warn("NOT saving variable {} for not yet started transaction {}", name, transaction);
            }
        }
    }

    public void processWorkflowInstance(DocumentContext json) {
        String bpmnProcessId = json.read("$.value.bpmnProcessId");
        String bpmnElementType = json.read("$.value.bpmnElementType");
        String intent = json.read("$.intent");

        if (TRANSFER_BPMN_NAMES.contains(bpmnProcessId)) {
            Long workflowInstanceKey = json.read("$.value.workflowInstanceKey");

            if ("PROCESS".equals(bpmnElementType)) {
                Long timestamp = json.read("$.timestamp");
                TransactionDirection direction = fromBpmn(bpmnProcessId);

                if ("ELEMENT_ACTIVATING".equals(intent)) {
                    inflightTransactionManager.processStarted(workflowInstanceKey, timestamp, direction);
                } else if ("ELEMENT_COMPLETED".equals(intent)) {
                    inflightTransactionManager.processEnded(workflowInstanceKey, timestamp, direction);
                }
            }
        }
    }
}
