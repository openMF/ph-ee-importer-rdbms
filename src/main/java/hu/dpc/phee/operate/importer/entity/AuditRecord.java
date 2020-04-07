package hu.dpc.phee.operate.importer.entity;


import javax.persistence.Cacheable;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "audit_records")
@Cacheable(false)
public class AuditRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    private Long workflowKey;
    private Long workflowInstanceKey;
    private Long timestamp;

    @Override
    public String toString() {
        return "AuditRecord{" +
                "id=" + id +
                ", workflowKey=" + workflowKey +
                ", workflowInstanceKey=" + workflowInstanceKey +
                ", timestamp=" + timestamp +
                '}';
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }


    public Long getWorkflowKey() {
        return workflowKey;
    }

    public void setWorkflowKey(Long workflowKey) {
        this.workflowKey = workflowKey;
    }

    public Long getWorkflowInstanceKey() {
        return workflowInstanceKey;
    }

    public void setWorkflowInstanceKey(Long workflowInstanceKey) {
        this.workflowInstanceKey = workflowInstanceKey;
    }
}
