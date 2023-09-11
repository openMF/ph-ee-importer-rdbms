package hu.dpc.phee.operator.entity.task;

import java.util.List;
import org.springframework.data.repository.CrudRepository;

public interface TaskRepository extends CrudRepository<Task, Long> {

    List<Task> findByWorkflowInstanceKey(Long workflowInstanceKey);

}
