package hu.dpc.phee.operator.entity.batch;

import hu.dpc.phee.operator.entity.parent.AbstractPersistableCustom;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.eclipse.persistence.annotations.Index;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import java.util.Date;

@Entity
@Table(name = "batches")
@Getter
@Setter
@NoArgsConstructor
public class Batch extends AbstractPersistableCustom<Long> {
    @Column(name = "BATCH_ID")
    private String batchId;

    @Column(name = "SUB_BATCH_ID")
    private String subBatchId;

    @Column(name = "REQUEST_ID")
    private String requestId;

    @Column(name = "REQUEST_FILE")
    private String requestFile;

    @Column(name = "TOTAL_TRANSACTIONS")
    private Long totalTransactions;

    @Column(name = "ONGOING")
    private Long ongoing;

    @Column(name = "FAILED")
    private Long failed;

    @Column(name = "COMPLETED")
    private Long completed;

    @Column(name = "TOTAL_AMOUNT")
    private Long totalAmount;

    @Column(name = "ONGOING_AMOUNT")
    private Long ongoingAmount;

    @Column(name = "FAILED_AMOUNT")
    private Long failedAmount;

    @Column(name = "COMPLETED_AMOUNT")
    private Long completedAmount;

    @Column(name = "RESULT_FILE")
    private String result_file;

    @Column(name = "RESULT_GENERATED_AT")
    private Date resultGeneratedAt;

    @Column(name = "NOTE")
    private String note;

    @Column(name = "WORKFLOW_KEY")
    private Long workflowKey;

    @Column(name = "WORKFLOW_INSTANCE_KEY")
    @Index(name = "idx_batches_key")
    private Long workflowInstanceKey;

    @Column(name = "STARTED_AT")
    private Date startedAt;

    @Column(name = "COMPLETED_AT")
    private Date completedAt;

    @Column(name = "PAYMENT_MODE")
    private String paymentMode;

    public Batch(Long workflowInstanceKey) {
        this.workflowInstanceKey = workflowInstanceKey;
    }
}
