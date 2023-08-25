package hu.dpc.phee.operator.entity.task;


import hu.dpc.phee.operator.entity.parent.AbstractPersistableCustom;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.With;

@Entity
@Table(name = "tasks")
@With
public class Task extends AbstractPersistableCustom<Long> {

    @Column(name = "WORKFLOW_KEY")
    private Long workflowKey;
    @Id
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
    @Column(name = "ELEMENT_ID")
    private String elementId;

    public Task() {
    }

    public Long getWorkflowKey() {
        return workflowKey;
    }

    public Long getWorkflowInstanceKey() {
        return workflowInstanceKey;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public String getIntent() {
        return intent;
    }

    public String getRecordType() {
        return recordType;
    }

    public String getType() {
        return type;
    }

    public String getElementId() {
        return elementId;
    }

    public void setWorkflowKey(Long workflowKey) {
        this.workflowKey = workflowKey;
    }

    public void setWorkflowInstanceKey(Long workflowInstanceKey) {
        this.workflowInstanceKey = workflowInstanceKey;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public void setIntent(String intent) {
        this.intent = intent;
    }

    public void setRecordType(String recordType) {
        this.recordType = recordType;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setElementId(String elementId) {
        this.elementId = elementId;
    }

    public Task(Long workflowKey, Long workflowInstanceKey, Long timestamp, String intent, String recordType, String type, String elementId) {
        this.workflowKey = workflowKey;
        this.workflowInstanceKey = workflowInstanceKey;
        this.timestamp = timestamp;
        this.intent = intent;
        this.recordType = recordType;
        this.type = type;
        this.elementId = elementId;
    }

//    public Task withWorkflowKey(Long workflowKey) {
//        return this.workflowKey == workflowKey ? this : new Task(workflowKey,this.workflowInstanceKey, this.timestamp, this.intent,this.recordType,this.type,this.elementId);
//    }
}
