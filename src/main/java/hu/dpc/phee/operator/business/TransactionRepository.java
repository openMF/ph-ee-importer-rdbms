package hu.dpc.phee.operator.business;

import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.stereotype.Repository;

import javax.transaction.Transactional;
import java.util.List;

@Repository
@Transactional
public interface TransactionRepository extends PagingAndSortingRepository<Transaction, Long> {

    Transaction findFirstByWorkflowInstanceKey(Long workflowInstanceKey);

    List<Transaction> findAllByCompletedAtNotNull(Pageable pageable);
}
