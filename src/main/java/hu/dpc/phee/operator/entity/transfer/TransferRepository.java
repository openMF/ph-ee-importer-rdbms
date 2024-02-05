package hu.dpc.phee.operator.entity.transfer;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import java.util.Optional;

public interface TransferRepository extends JpaRepository<Transfer, Long>, JpaSpecificationExecutor {

    Optional<Transfer> findByWorkflowInstanceKey(Long workflowInstanceKey);
    Optional<Transfer> findByTransactionIdAndBatchId(String transactionId, String batchId);
}
