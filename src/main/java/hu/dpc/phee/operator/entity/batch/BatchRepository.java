package hu.dpc.phee.operator.entity.batch;

import org.springframework.data.repository.CrudRepository;

public interface BatchRepository extends CrudRepository<Batch, Long> {

    Batch findByWorkflowInstanceKey(Long workflowInstanceKey);

}
