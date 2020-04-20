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
public class OutgoingTransactionParser {
    private static Logger logger = LoggerFactory.getLogger(OutgoingTransactionParser.class);

    private static Map<String, Consumer<Pair<Transaction, String>>> VARIABLE_PARSERS = new HashMap<>();

    static {
        VARIABLE_PARSERS.put("transactionId", pair -> pair.getFirst().setTransactionId(strip(pair.getSecond())));
        VARIABLE_PARSERS.put("payeeFspId", pair -> pair.getFirst().setPayeeDfspId(strip(pair.getSecond())));
        VARIABLE_PARSERS.put("transactionRequest", pair -> parseTransactionRequest(pair.getFirst(), pair.getSecond()));
        VARIABLE_PARSERS.put("payeeQuoteResponse", pair -> parsePayeeQuoteResponse(pair.getFirst(), pair.getSecond()));
        VARIABLE_PARSERS.put("localQuoteResponse", pair -> parseLocalQuoteResponse(pair.getFirst(), pair.getSecond()));
        VARIABLE_PARSERS.put("transferResponse-PREPARE", pair -> parseTransferResponsePrepare(pair.getFirst(), pair.getSecond()));
    }


    @Autowired
    private TransactionRepository transactionRepository;

    /**
     * in-memory in-flight transactions
     */
    private Map<Long, Transaction> inflightTransactions = new HashMap<>();


    private static void parseTransferResponsePrepare(Transaction transaction, String jsonString) {
        DocumentContext json = JsonPathReader.parseEscaped(jsonString);
        String completedAt = json.read("$.completedTimestamp", String.class);
        SimpleDateFormat dateFormat = OperatorUtils.dateFormat();
        try {
            transaction.setCompletedAt(dateFormat.parse(completedAt));
        } catch (ParseException e) {
            logger.error("failed to parse completedTimestamp", e);
        }
    }

    private static void parseLocalQuoteResponse(Transaction transaction, String jsonString) {
        DocumentContext json = JsonPathReader.parseEscaped(jsonString);
        transaction.setPayerFee(json.read("$.fspFee.amount", BigDecimal.class));
        transaction.setPayerFeeCurrency(json.read("$.fspFee.currency"));
        transaction.setPayerQuoteCode(json.read("$.quoteCode"));
    }

    private static void parsePayeeQuoteResponse(Transaction transaction, String jsonString) {
        DocumentContext json = JsonPathReader.parseEscaped(jsonString);
        transaction.setPayeeFee(json.read("$.payeeFspFee.amount", BigDecimal.class));
        transaction.setPayeeFeeCurrency(json.read("$.payeeFspFee.currency"));
    }

    private static void parseTransactionRequest(Transaction transaction, String jsonString) {
        DocumentContext json = JsonPathReader.parseEscaped(jsonString);

        transaction.setPayerDfspId(json.read("$.payer.partyIdInfo.fspId"));
        transaction.setPayerPartyId(json.read("$.payer.partyIdInfo.partyIdentifier"));
        transaction.setPayerPartyIdType(json.read("$.payer.partyIdInfo.partyIdType"));

        transaction.setPayeePartyId(json.read("$.payee.partyIdInfo.partyIdentifier"));
        transaction.setPayeePartyIdType(json.read("$.payee.partyIdInfo.partyIdType"));

        transaction.setAmount(json.read("$.amount.amount", BigDecimal.class));
        transaction.setCurrency(json.read("$.amount.currency"));
    }


    public void parseVariable(DocumentContext json) {
        String name = json.read("$.value.name");

        if (VARIABLE_PARSERS.keySet().contains(name)) {
            Long workflowInstanceKey = json.read("$.value.workflowInstanceKey");
            String value = json.read("$.value.value");

            Transaction transaction = getOrCreateTransaction(workflowInstanceKey);
            VARIABLE_PARSERS.get(name).accept(Pair.of(transaction, value));
        }
    }

    public void parseWorkflowElement(DocumentContext json) {
        String bpmnElementType = json.read("$.value.bpmnElementType");
        String intent = json.read("$.intent");

        if ("START_EVENT".equals(bpmnElementType) && "ELEMENT_ACTIVATED".equals(intent)) {
            Long workflowInstanceKey = json.read("$.value.workflowInstanceKey");
            Long timestamp = json.read("$.timestamp");
            Transaction transaction = getOrCreateTransaction(workflowInstanceKey);
            transaction.setStartedAt(new Date(timestamp));
            inflightTransactions.put(workflowInstanceKey, transaction);
            logger.debug("started in-flight transaction {}", transaction.getWorkflowInstanceKey());
        }

        if ("END_EVENT".equals(bpmnElementType) && "ELEMENT_ACTIVATED".equals(intent)) {
            Long workflowInstanceKey = json.read("$.value.workflowInstanceKey");
            Transaction transaction = inflightTransactions.remove(workflowInstanceKey);
            if (transaction == null) {
                logger.error("failed to find in-flight transaction {}", workflowInstanceKey);
            } else {
                transactionRepository.save(transaction);
                logger.debug("saved finished transaction {}", transaction.getWorkflowInstanceKey());
            }
        }
    }

    /**
     * check if send to channel JOBs are COMPLETED
     */
    public void checkTransactionStatus(DocumentContext json) {
        String type = json.read("$.value.type");
        String intent = json.read("$.intent");
        if ("COMPLETED".equals(intent)) {
            if ("send-success-to-channel".equals(type)) {
                transactionStatus(json, TransactionStatus.COMPLETED);
            }
            if ("send-error-to-channel".equals(type)) {
                transactionStatus(json, TransactionStatus.FAILED);
            }
        }
    }


    public void transactionStatus(DocumentContext json, TransactionStatus status) {
        Long workflowInstanceKey = json.read("$.value.workflowInstanceKey");
        Transaction transaction = getOrCreateTransaction(workflowInstanceKey);
        transaction.setStatus(status);
        logger.debug("transaction {} set to {}", workflowInstanceKey, status);
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
