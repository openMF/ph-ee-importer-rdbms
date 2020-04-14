package hu.dpc.phee.operator.audit;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import javax.transaction.Transactional;
import java.util.List;

@Repository
@Transactional
public interface TaskRepository extends CrudRepository<Task, Long> {

    List<Task> findByWorkflowInstanceKeyOrderByTimestamp(Long workflowInstanceKey);

}
