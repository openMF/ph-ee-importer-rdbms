package hu.dpc.phee.operator.entity.transfer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

public interface TransferRepository extends JpaRepository<Transfer, Long>, JpaSpecificationExecutor {
    Logger logger = LoggerFactory.getLogger(TransferRepository.class);

    Transfer findByWorkflowInstanceKey(Long workflowInstanceKey);


    @Transactional
    default Transfer saveIfFresh(Transfer transfer) {
        Optional<Transfer> existingTransfer = findById(transfer.getWorkflowInstanceKey());
        if (existingTransfer.isEmpty() || existingTransfer.get().getLastUpdated() < transfer.getLastUpdated()) {
            return save(transfer);
        } else {
            logger.warn("not merging obsolete transfer: {}", transfer);
            return existingTransfer.get();
        }
    }
}
