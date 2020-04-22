package hu.dpc.phee.operator.importer;

import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operator.OperatorUtils;
import hu.dpc.phee.operator.business.Transaction;
import hu.dpc.phee.operator.business.TransactionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static hu.dpc.phee.operator.OperatorUtils.strip;

@Component
public class OutgoingVariableParser {
    private static Logger logger = LoggerFactory.getLogger(OutgoingVariableParser.class);

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
    private InflightTransactionManager inflightTransactionManager;

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
            logger.debug("parsing OUTGOING variable {}", name);
            Long workflowInstanceKey = json.read("$.value.workflowInstanceKey");
            String value = json.read("$.value.value");

            Transaction transaction = inflightTransactionManager.getOrCreateTransaction(workflowInstanceKey);
            VARIABLE_PARSERS.get(name).accept(Pair.of(transaction, value));
            transactionRepository.save(transaction);
        }
    }
}
