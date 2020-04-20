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

    @Autowired
    private TransactionRepository transactionRepository;

    /**
     * in-memory in-flight transactions
     */
    private Map<Long, Transaction> inflightTransactions = new HashMap<>();


    public void parseVariable(DocumentContext json) {
        logger.debug("## parse INCOMING variable");
    }

    public void parseWorkflowElement(DocumentContext json) {
        logger.debug("## parse INCOMING workflow element");
    }

    public void checkTransactionStatus(DocumentContext json) {
        logger.debug("## check INCOMING transaction status");
    }
}
