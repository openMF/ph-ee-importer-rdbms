package hu.dpc.phee.operator.entity.transactionrequest;

import org.springframework.data.repository.CrudRepository;

public interface TransactionRequestRepository extends CrudRepository<TransactionRequest, Long> {

    TransactionRequest findByWorkflowInstanceKey(Long workflowInstanceKey);

}
