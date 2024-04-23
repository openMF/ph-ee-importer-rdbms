package hu.dpc.phee.operator.entity.variable;

import jakarta.persistence.EntityManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.repository.CrudRepository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

public interface VariableRepository extends CrudRepository<Variable, Long> {
    Logger logger = LoggerFactory.getLogger(VariableRepository.class);

    List<Variable> findByWorkflowInstanceKey(Long workflowInstanceKey);


    @Transactional
    default Variable saveIfFresh(Variable variable) {
        EntityManager entityManager = getEntityManager();
        VariableId id = new VariableId(variable.getWorkflowInstanceKey(), variable.getName());

        Variable existingVariable = entityManager.find(Variable.class, id);
        if (existingVariable == null || existingVariable.getTimestamp() < variable.getTimestamp()) {
            return entityManager.merge(variable);
        } else {
            logger.warn("not merging obsolete variable: {}", id);
            return existingVariable;
        }
    }

    EntityManager getEntityManager();
}
