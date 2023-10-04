package hu.dpc.phee.operator.streams;

import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operator.config.TransferTransformerConfig;
import hu.dpc.phee.operator.entity.transactionrequest.TransactionRequest;
import hu.dpc.phee.operator.entity.transactionrequest.TransactionRequestRepository;
import hu.dpc.phee.operator.entity.transactionrequest.TransactionRequestState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Optional;

public class InflightTransactionRequestManager {

    @Autowired
    TransactionRequestRepository transactionRequestRepository;

    @Autowired
    TransferTransformerConfig transferTransformerConfig;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public TransactionRequest retrieveOrCreateTransaction(String bpmn, DocumentContext record) {
        Long processInstanceKey = record.read("$.value.processInstanceKey", Long.class);
        Optional<TransferTransformerConfig.Flow> config = transferTransformerConfig.findFlow(bpmn);
        // This code block should also process transaction/batch/outboundMsg Type
        TransactionRequest transactionRequest = transactionRequestRepository.findByWorkflowInstanceKey(processInstanceKey);
        if (transactionRequest == null) {
            logger.debug("creating new Transaction for processInstanceKey: {}", processInstanceKey);
            transactionRequest = new TransactionRequest(processInstanceKey);
            transactionRequest.setState(TransactionRequestState.IN_PROGRESS);

            if (config.isPresent()) {
                transactionRequest.setDirection(config.get().getDirection());
            } else {
                logger.error("No config found for bpmn: {}", bpmn);
            }
            transactionRequestRepository.save(transactionRequest);
        } else {
            logger.info("found existing Transaction for processInstanceKey: {}", processInstanceKey);
        }
        return transactionRequest;
    }
}
