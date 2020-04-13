package hu.dpc.phee.operator.business;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import javax.transaction.Transactional;

@Repository
@Transactional
public interface TransactionRepository extends CrudRepository<Transaction, Long> {

    Transaction findFirstByWorkflowInstanceKey(Long workflowInstanceKey);

}
