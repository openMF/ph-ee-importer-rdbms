package hu.dpc.phee.operator.entity.variable;


import hu.dpc.phee.operator.entity.parent.AbstractPersistableCustom;
import lombok.Getter;
import lombok.Setter;
import org.eclipse.persistence.annotations.Index;

import javax.persistence.*;

@Entity
@Table(name = "variables")
@Cacheable(false)
@Getter
@Setter
public class Variable extends AbstractPersistableCustom<Long> {

    @Column(name = "WORKFLOW_KEY")
    private Long workflowKey;

    @Column(name = "WORKFLOW_INSTANCE_KEY")
    @Index(name = "idx_workflowInstanceKey")
    private Long workflowInstanceKey;

    @Column(name = "TIMESTAMP")
    private Long timestamp;

    @Column(name = "NAME")
    private String name;

    @Lob
    @Column(name = "VALUE")
    private String value;
}
