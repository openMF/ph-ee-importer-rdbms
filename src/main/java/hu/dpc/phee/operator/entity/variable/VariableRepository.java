package hu.dpc.phee.operator.entity.variable;

import org.springframework.data.repository.CrudRepository;

import java.util.List;

public interface VariableRepository extends CrudRepository<Variable, Long> {

    List<Variable> findByWorkflowInstanceKey(Long workflowInstanceKey);

}
