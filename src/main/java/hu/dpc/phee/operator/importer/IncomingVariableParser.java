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
public class IncomingVariableParser {
    private static Logger logger = LoggerFactory.getLogger(IncomingVariableParser.class);

    private static Map<String, Consumer<Pair<Transaction, String>>> VARIABLE_PARSERS = new HashMap<>();

    static {
        VARIABLE_PARSERS.put("quoteSwitchRequest", pair -> parseQuoteSwitchRequest(pair.getFirst(), strip(pair.getSecond())));
        VARIABLE_PARSERS.put("transferResponse-CREATE", pair -> parseTransferResponse(pair.getFirst(), strip(pair.getSecond())));
        VARIABLE_PARSERS.put("localQuoteResponse", pair -> parseLocalQuoteResponse(pair.getFirst(), strip(pair.getSecond())));
    }


    @Autowired
    private TransactionManager transactionManager;

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
        String name = json.read("$.value.name");

        if (VARIABLE_PARSERS.keySet().contains(name)) {
            logger.debug("parsing INCOMING variable {}", name);
            Long workflowInstanceKey = json.read("$.value.workflowInstanceKey");
            String value = json.read("$.value.value");

            Transaction transaction = transactionManager.getOrCreateTransaction(workflowInstanceKey);
            VARIABLE_PARSERS.get(name).accept(Pair.of(transaction, value));
            transactionRepository.save(transaction);
        }
    }
}