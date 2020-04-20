package hu.dpc.phee.operator.business;

import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operator.OperatorUtils;
import hu.dpc.phee.operator.importer.JsonPathReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static hu.dpc.phee.operator.OperatorUtils.strip;

@Component
public class IncomingTransactionParser {
    private static Logger logger = LoggerFactory.getLogger(IncomingTransactionParser.class);

    public static final String BPMN_PAYEE_QUOTE_TRANSFER = "PayeeQuoteTransfer-DFSPID";

    private static Map<String, Consumer<Pair<Transaction, String>>> VARIABLE_PARSERS = new HashMap<>();

    static {
        VARIABLE_PARSERS.put("quoteSwitchRequest", pair -> parseQuoteSwitchRequest(pair.getFirst(), strip(pair.getSecond())));
        VARIABLE_PARSERS.put("transferResponse-CREATE", pair -> parseTransferResponse(pair.getFirst(), strip(pair.getSecond())));
        VARIABLE_PARSERS.put("localQuoteResponse", pair -> parseLocalQuoteResponse(pair.getFirst(), strip(pair.getSecond())));
    }



    @Autowired
    private TransactionRepository transactionRepository;

    /**
     * in-memory in-flight transactions
     */
    private Map<Long, Transaction> inflightTransactions = new HashMap<>();


    private static void parseLocalQuoteResponse(Transaction transaction, String jsonString) {
        DocumentContext json = JsonPathReader.parseEscaped(jsonString);
        transaction.setPayeeFee(json.read("$.fspFee.amount", BigDecimal.class));
        transaction.setPayeeFeeCurrency(json.read("$.fspFee.currency"));
        transaction.setPayeeQuoteCode(json.read("$.quoteCode"));
    }

    private static void parseQuoteSwitchRequest(Transaction transaction, String jsonString) {
        DocumentContext json = JsonPathReader.parseEscaped(jsonString);
        transaction.setTransactionId(json.read("$.transactionId"));

        transaction.setPayeePartyIdType(json.read("$.payee.partyIdInfo.partyIdType"));
        transaction.setPayeePartyId(json.read("$.payee.partyIdInfo.partyIdentifier"));
        transaction.setPayeeDfspId(json.read("$.payee.partyIdInfo.fspId"));

        transaction.setPayerPartyIdType(json.read("$.payer.partyIdInfo.partyIdType"));
        transaction.setPayerPartyId(json.read("$.payer.partyIdInfo.partyIdentifier"));
        transaction.setPayerDfspId(json.read("$.payer.partyIdInfo.fspId"));

        transaction.setAmount(json.read("$.amount.amount", BigDecimal.class));
        transaction.setCurrency(json.read("$.amount.currency"));
    }

    private static void parseTransferResponse(Transaction transaction, String jsonString) {
        DocumentContext json = JsonPathReader.parseEscaped(jsonString);
        String completedAt = json.read("$.completedTimestamp", String.class);
        SimpleDateFormat dateFormat = OperatorUtils.dateFormat();
        try {
            transaction.setCompletedAt(dateFormat.parse(completedAt));
        } catch (ParseException e) {
            logger.error("failed to parse completedTimestamp", e);
        }
    }

    public void parseVariable(DocumentContext json) {
        logger.debug("## parse INCOMING variable");
        String name = json.read("$.value.name");

        if (VARIABLE_PARSERS.keySet().contains(name)) {
            Long workflowInstanceKey = json.read("$.value.workflowInstanceKey");
            String value = json.read("$.value.value");

            Transaction transaction = getOrCreateTransaction(workflowInstanceKey);
            VARIABLE_PARSERS.get(name).accept(Pair.of(transaction, value));
        }
    }

    public void parseWorkflowElement(DocumentContext json) {
        logger.debug("## parse INCOMING workflow element");

        String bpmnElementType = json.read("$.value.bpmnElementType");
        String bpmnProcessId = json.read("$.value.bpmnProcessId");
        String intent = json.read("$.intent");
        Long workflowInstanceKey = json.read("$.value.workflowInstanceKey");

        if (BPMN_PAYEE_QUOTE_TRANSFER.equals(bpmnProcessId) && "START_EVENT".equals(bpmnElementType) && "ELEMENT_ACTIVATED".equals(intent)) {
            Long timestamp = json.read("$.timestamp");
            Transaction transaction = getOrCreateTransaction(workflowInstanceKey);
            transaction.setStartedAt(new Date(timestamp));
            inflightTransactions.put(workflowInstanceKey, transaction);
            logger.debug("started in-flight INCOMING transaction {}", transaction.getWorkflowInstanceKey());
        }

        if (BPMN_PAYEE_QUOTE_TRANSFER.equals(bpmnProcessId) && "END_EVENT".equals(bpmnElementType) && "ELEMENT_ACTIVATED".equals(intent)) {
            Transaction transaction = inflightTransactions.remove(workflowInstanceKey);
            if (transaction == null) {
                logger.error("failed to find in-flight INCOMING transaction {}", workflowInstanceKey);
            } else {
                transaction.setStatus(TransactionStatus.COMPLETED);
                transactionRepository.save(transaction);
                logger.debug("saved finished INCOMING transaction {}", transaction.getWorkflowInstanceKey());
            }
        }
    }

    public void checkTransactionStatus(DocumentContext json) {
        String bpmnProcessId = json.read("$.value.bpmnProcessId");
        if (BPMN_PAYEE_QUOTE_TRANSFER.equals(bpmnProcessId)) {

        }
        logger.debug("## check INCOMING transaction status");
    }


    private synchronized Transaction getOrCreateTransaction(Long workflowInstanceKey) {
        Transaction transaction = inflightTransactions.get(workflowInstanceKey);
        if (transaction == null) {
            transaction = new Transaction(workflowInstanceKey);
            inflightTransactions.put(workflowInstanceKey, transaction);
        }
        return transaction;
    }
}
