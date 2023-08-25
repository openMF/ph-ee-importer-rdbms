package hu.dpc.phee.operator.entity.variable;


import hu.dpc.phee.operator.entity.parent.AbstractPersistableCustom;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.With;
import org.eclipse.persistence.annotations.Index;

@Entity
@IdClass(VariableId.class)
@Table(name = "variables")
@Cacheable(false)
@Data
@With
@NoArgsConstructor
@AllArgsConstructor
public class Variable {

    @Id
    @Column(name = "WORKFLOW_INSTANCE_KEY")
    private Long workflowInstanceKey;

    @Id
    @Column(name = "NAME")
    private String name;

    @Column(name = "WORKFLOW_KEY")
    private Long workflowKey;

    @Column(name = "TIMESTAMP")
    private Long timestamp;

    @Lob
    @Column(name = "VALUE")
    private String value;

    public Long getWorkflowInstanceKey() {
        return workflowInstanceKey;
    }

    public String getName() {
        return name;
    }

    public Long getWorkflowKey() {
        return workflowKey;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public String getValue() {
        return value;
    }

    public void setWorkflowInstanceKey(Long workflowInstanceKey) {
        this.workflowInstanceKey = workflowInstanceKey;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setWorkflowKey(Long workflowKey) {
        this.workflowKey = workflowKey;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
