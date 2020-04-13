package hu.dpc.phee.operator.audit;


import javax.persistence.Cacheable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "tasks")
@Cacheable(false)
public class Task {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    @Column(name = "WORKFLOW_KEY")
    private Long workflowKey;
    @Column(name = "WORKFLOW_INSTANCE_KEY")
    private Long workflowInstanceKey;
    @Column(name = "TIMESTAMP")
    private Long timestamp;
    @Column(name = "INTENT")
    private String intent;
    @Column(name = "RECORD_TYPE")
    private String recordType;
    @Column(name = "TYPE")
    private String type;


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

    public String getIntent() {
        return intent;
    }

    public void setIntent(String intent) {
        this.intent = intent;
    }

    public String getRecordType() {
        return recordType;
    }

    public void setRecordType(String recordType) {
        this.recordType = recordType;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
