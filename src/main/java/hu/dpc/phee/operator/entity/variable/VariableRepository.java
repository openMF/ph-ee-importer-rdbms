package hu.dpc.phee.operator.entity.variable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.repository.CrudRepository;

import java.util.List;
import java.util.Optional;

public interface VariableRepository extends CrudRepository<Variable, Long> {
    Logger logger = LoggerFactory.getLogger(VariableRepository.class);

    List<Variable> findByWorkflowInstanceKey(Long workflowInstanceKey);

    Optional<Variable> findByWorkflowInstanceKeyAndName(Long workflowInstanceKey, String name);


    default Variable saveIfFresh(Variable variable) {
        Optional<Variable> existingVariable = findByWorkflowInstanceKeyAndName(variable.getWorkflowInstanceKey(), variable.getName());

        if (existingVariable.isEmpty() || existingVariable.get().getPosition() <= variable.getPosition()) {
            return save(variable);
        } else {
            logger.warn("not merging obsolete variable: {} for workflow instance: {} (old position: {}, new position: {})",
                    variable.getName(), variable.getWorkflowInstanceKey(), existingVariable.get().getPosition(), variable.getPosition());
            return existingVariable.orElse(null);
        }
    }
}
