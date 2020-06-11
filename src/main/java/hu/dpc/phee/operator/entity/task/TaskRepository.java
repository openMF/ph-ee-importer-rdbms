package hu.dpc.phee.operator.entity.task;

import org.springframework.data.repository.CrudRepository;

import java.util.List;

public interface TaskRepository extends CrudRepository<Task, Long> {

    List<Task> findByWorkflowInstanceKey(Long workflowInstanceKey);

}
