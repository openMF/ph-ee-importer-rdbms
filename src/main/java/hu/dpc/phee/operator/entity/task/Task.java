package hu.dpc.phee.operator.entity.task;


import hu.dpc.phee.operator.entity.parent.AbstractPersistableCustom;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name = "tasks")
@Getter
@Setter
public class Task extends AbstractPersistableCustom<Long> {
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
    @Column(name = "ELEMENT_ID")
    private String elementId;
}
