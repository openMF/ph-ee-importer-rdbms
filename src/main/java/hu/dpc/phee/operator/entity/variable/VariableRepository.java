package hu.dpc.phee.operator.entity.variable;

import java.util.List;
import org.springframework.data.repository.CrudRepository;

public interface VariableRepository extends CrudRepository<Variable, Long> {

    List<Variable> findByWorkflowInstanceKey(Long workflowInstanceKey);

}
