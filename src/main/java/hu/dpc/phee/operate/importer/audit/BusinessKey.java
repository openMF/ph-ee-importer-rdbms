package hu.dpc.phee.operate.importer.audit;


import org.eclipse.persistence.annotations.Index;

import javax.persistence.Cacheable;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "businesskeys")
@Cacheable(false)
public class BusinessKey {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    @Index(name = "idx_businessKey")
    private String businessKey;

    @Index(name = "idx_businessKeyType")
    private String businessKeyType;

    @Index(name = "idx_workflowInstanceKey")
    private Long workflowInstanceKey;
    private Long timestamp;

    public Long getId() {
        return id;
    }

    public String getBusinessKeyType() {
        return businessKeyType;
    }

    public void setBusinessKeyType(String businessKeyType) {
        this.businessKeyType = businessKeyType;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getWorkflowInstanceKey() {
        return workflowInstanceKey;
    }

    public void setWorkflowInstanceKey(Long workflowInstanceKey) {
        this.workflowInstanceKey = workflowInstanceKey;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getBusinessKey() {
        return businessKey;
    }

    public void setBusinessKey(String transactionId) {
        this.businessKey = transactionId;
    }
}
