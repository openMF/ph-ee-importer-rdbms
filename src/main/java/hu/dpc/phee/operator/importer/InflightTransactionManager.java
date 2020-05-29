package hu.dpc.phee.operator.importer;

import hu.dpc.phee.operator.business.Transaction;
import hu.dpc.phee.operator.business.TransactionDirection;
import hu.dpc.phee.operator.business.TransactionRepository;
import hu.dpc.phee.operator.business.TransactionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Component
public class InflightTransactionManager {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private TransactionRepository transactionRepository;

    /**
     * in-memory in-flight transactions
     */
    private Map<Long, Transaction> inflightTransactions = new HashMap<>();


    public void processStarted(Long workflowInstanceKey, Long timestamp) {
        Transaction transaction = getOrCreateTransaction(workflowInstanceKey);
        transaction.setStartedAt(new Date(timestamp));
        transactionRepository.save(transaction);
        logger.debug("started in-flight {} transaction {}", transaction.getDirection(), transaction.getWorkflowInstanceKey());
    }

    public void processEnded(Long workflowInstanceKey, Long timestamp) {
        Transaction transaction = inflightTransactions.remove(workflowInstanceKey);
        if (transaction == null) {
            logger.error("failed to find in-flight transaction {}", workflowInstanceKey);
        } else {
            TransactionDirection direction = transaction.getDirection();
            transaction.setCompletedAt(new Date(timestamp));
            transactionRepository.save(transaction);
            logger.debug("saved finished {} transaction {}", direction, transaction.getWorkflowInstanceKey());
        }
    }

    public void transactionResult(Long workflowInstanceKey, TransactionStatus status, String endElementId) {
        Transaction transaction = getOrCreateTransaction(workflowInstanceKey);
        transaction.setStatus(status);
        transaction.setStatusDetail(endElementId);  // TODO turn into human readable
        transactionRepository.save(transaction);
        logger.debug("{} transaction {} result {} ({})", transaction.getDirection(), workflowInstanceKey, status, endElementId);
    }

    synchronized Transaction getOrCreateTransaction(Long workflowInstanceKey) {
        Transaction transaction = inflightTransactions.get(workflowInstanceKey);
        if (transaction == null) {
            transaction = new Transaction(workflowInstanceKey);
            inflightTransactions.put(workflowInstanceKey, transaction);
        }
        return transaction;
    }
}
