package hu.dpc.phee.operator.entity.card;

import org.springframework.data.jpa.repository.JpaRepository;

public interface CardTransactionRepository extends JpaRepository<CardTransaction, String> {

    CardTransaction findByWorkflowInstanceKey(String workflowInstanceKey);

}