package hu.dpc.phee.operator.streams;

import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operator.config.TransferTransformerConfig;
import hu.dpc.phee.operator.entity.transfer.Transfer;
import hu.dpc.phee.operator.entity.transfer.TransferRepository;
import hu.dpc.phee.operator.entity.transfer.TransferStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Optional;

public class InFlightTransferManager {

    @Autowired
    TransferTransformerConfig transferTransformerConfig;

    @Autowired
    TransferRepository transferRepository;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public Transfer retrieveOrCreateTransfer(String bpmn, DocumentContext record) {
        Long processInstanceKey = record.read("$.value.processInstanceKey", Long.class);
        Optional<TransferTransformerConfig.Flow> config = transferTransformerConfig.findFlow(bpmn);
        // This code block should also process transaction/batch/outboundMsg Type
        Transfer transfer = transferRepository.findByWorkflowInstanceKey(processInstanceKey);
        if (transfer == null) {
            logger.debug("creating new Transfer for processInstanceKey: {}", processInstanceKey);
            transfer = new Transfer(processInstanceKey);
            transfer.setStatus(TransferStatus.IN_PROGRESS);

            if (config.isPresent()) {
                transfer.setDirection(config.get().getDirection());
            } else {
                logger.error("No config found for bpmn: {}", bpmn);
            }
            transferRepository.save(transfer);
        } else {
            logger.info("found existing Transfer for processInstanceKey: {}", processInstanceKey);
        }
        return transfer;
    }
}
