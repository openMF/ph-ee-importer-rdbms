package hu.dpc.phee.operator.entity.transfer;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.transaction.annotation.Transactional;

public interface TransferRepository extends JpaRepository<Transfer, Long>, JpaSpecificationExecutor {
    Logger logger = LoggerFactory.getLogger(TransferRepository.class);

    Transfer findByWorkflowInstanceKey(Long workflowInstanceKey);


    @Transactional
    default Transfer saveIfFresh(Transfer transfer) {
        EntityManager entityManager = entityManager();

        Transfer existingTransfer = entityManager.find(Transfer.class, transfer.getWorkflowInstanceKey());
        if (existingTransfer == null || existingTransfer.getLastUpdated() < transfer.getLastUpdated()) {
            return entityManager.merge(transfer);
        } else {
            logger.warn("not merging obsolete transfer: {}", transfer);
            return existingTransfer;
        }
    }

    @PersistenceContext
    EntityManager entityManager();
}
