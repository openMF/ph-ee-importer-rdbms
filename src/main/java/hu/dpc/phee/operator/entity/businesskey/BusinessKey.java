package hu.dpc.phee.operator.entity.businesskey;


import hu.dpc.phee.operator.entity.parent.AbstractPersistableCustom;
import lombok.Getter;
import lombok.Setter;
import org.eclipse.persistence.annotations.Index;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name = "businesskeys")
@Getter
@Setter
public class BusinessKey extends AbstractPersistableCustom<Long> {

    @Column(name = "BUSINESS_KEY")
    @Index(name = "idx_businessKey")
    private String businessKey;

    @Column(name = "BUSINESS_KEY_TYPE")
    @Index(name = "idx_businessKeyType")
    private String businessKeyType;

    @Column(name = "WORKFLOW_INSTANCE_KEY")
    @Index(name = "idx_workflowInstanceKey")
    private Long workflowInstanceKey;

    @Column(name = "TIMESTAMP")
    private Long timestamp;
}
