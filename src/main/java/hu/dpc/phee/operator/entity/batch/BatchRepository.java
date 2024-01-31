package hu.dpc.phee.operator.entity.batch;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

import java.util.Optional;

public interface BatchRepository extends JpaRepository<Batch, Long>, JpaSpecificationExecutor {

    Batch findByWorkflowInstanceKey(Long workflowInstanceKey);
    Batch findByBatchId(String batchId);
    Batch findBySubBatchId(String subBatchId);
    Optional<Batch> findByBatchIdAndSubBatchIdIsNull(String batchId);
}
