package hu.dpc.phee.operator.entity.transfer;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

public interface TransferRepository extends JpaRepository<Transfer, Long>, JpaSpecificationExecutor {

    Transfer findByWorkflowInstanceKey(Long workflowInstanceKey);

}
