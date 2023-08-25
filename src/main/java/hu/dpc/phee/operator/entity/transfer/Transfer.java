package hu.dpc.phee.operator.entity.transfer;


import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.With;

import java.math.BigDecimal;
import java.util.Date;

@Entity
@Table(name = "transfers")
@Cacheable(false)
@Data
@With
@AllArgsConstructor
public class Transfer {

    @Id
    @Column(name = "WORKFLOW_INSTANCE_KEY")
    private Long workflowInstanceKey;

    @Column(name = "TRANSACTION_ID")
    private String transactionId;

    @Column(name = "STARTED_AT")
    private Date startedAt;
    @Column(name = "COMPLETED_AT")
    private Date completedAt;

    @Enumerated(EnumType.STRING)
    @Column(name = "status")
    private TransferStatus status;

    @Column(name = "STATUS_DETAIL")
    private String statusDetail;

    @Column(name = "PAYEE_DFSP_ID")
    private String payeeDfspId;
    @Column(name = "PAYEE_PARTY_ID")
    private String payeePartyId;
    @Column(name = "PAYEE_PARTY_ID_TYPE")
    private String payeePartyIdType;
    @Column(name = "PAYEE_FEE")
    private BigDecimal payeeFee;
    @Column(name = "PAYEE_FEE_CURRENCY")
    private String payeeFeeCurrency;
    @Column(name = "PAYEE_QUOTE_CODE")
    private String payeeQuoteCode;

    @Column(name = "PAYER_DFSP_ID")
    private String payerDfspId;
    @Column(name = "PAYER_PARTY_ID")
    private String payerPartyId;
    @Column(name = "PAYER_PARTY_ID_TYPE")
    private String payerPartyIdType;
    @Column(name = "PAYER_FEE")
    private BigDecimal payerFee;
    @Column(name = "PAYER_FEE_CURRENCY")
    private String payerFeeCurrency;
    @Column(name = "PAYER_QUOTE_CODE")
    private String payerQuoteCode;

    @Column(name = "amount")
    private BigDecimal amount;

    @Column(name = "currency")
    private String currency;

    @Column(name = "direction")
    private String direction;

    @Column(name = "error_information")
    private String errorInformation;

    @Column(name = "BATCH_ID")
    private String batchId;

    @Column(name = "CLIENTCORRELATIONID")
    private String clientCorrelationId;


    public Transfer(Long workflowInstanceKey) {
        this.workflowInstanceKey = workflowInstanceKey;
        this.status = TransferStatus.IN_PROGRESS;
    }

    public Transfer() {
    }

    public Long getWorkflowInstanceKey() {
        return workflowInstanceKey;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public Date getStartedAt() {
        return startedAt;
    }

    public Date getCompletedAt() {
        return completedAt;
    }

    public TransferStatus getStatus() {
        return status;
    }

    public String getStatusDetail() {
        return statusDetail;
    }

    public String getPayeeDfspId() {
        return payeeDfspId;
    }

    public String getPayeePartyId() {
        return payeePartyId;
    }

    public String getPayeePartyIdType() {
        return payeePartyIdType;
    }

    public BigDecimal getPayeeFee() {
        return payeeFee;
    }

    public String getPayeeFeeCurrency() {
        return payeeFeeCurrency;
    }

    public String getPayeeQuoteCode() {
        return payeeQuoteCode;
    }

    public String getPayerDfspId() {
        return payerDfspId;
    }

    public String getPayerPartyId() {
        return payerPartyId;
    }

    public String getPayerPartyIdType() {
        return payerPartyIdType;
    }

    public BigDecimal getPayerFee() {
        return payerFee;
    }

    public String getPayerFeeCurrency() {
        return payerFeeCurrency;
    }

    public String getPayerQuoteCode() {
        return payerQuoteCode;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public String getCurrency() {
        return currency;
    }

    public String getDirection() {
        return direction;
    }

    public String getErrorInformation() {
        return errorInformation;
    }

    public String getBatchId() {
        return batchId;
    }

    public String getClientCorrelationId() {
        return clientCorrelationId;
    }

    public void setWorkflowInstanceKey(Long workflowInstanceKey) {
        this.workflowInstanceKey = workflowInstanceKey;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public void setStartedAt(Date startedAt) {
        this.startedAt = startedAt;
    }

    public void setCompletedAt(Date completedAt) {
        this.completedAt = completedAt;
    }

    public void setStatus(TransferStatus status) {
        this.status = status;
    }

    public void setStatusDetail(String statusDetail) {
        this.statusDetail = statusDetail;
    }

    public void setPayeeDfspId(String payeeDfspId) {
        this.payeeDfspId = payeeDfspId;
    }

    public void setPayeePartyId(String payeePartyId) {
        this.payeePartyId = payeePartyId;
    }

    public void setPayeePartyIdType(String payeePartyIdType) {
        this.payeePartyIdType = payeePartyIdType;
    }

    public void setPayeeFee(BigDecimal payeeFee) {
        this.payeeFee = payeeFee;
    }

    public void setPayeeFeeCurrency(String payeeFeeCurrency) {
        this.payeeFeeCurrency = payeeFeeCurrency;
    }

    public void setPayeeQuoteCode(String payeeQuoteCode) {
        this.payeeQuoteCode = payeeQuoteCode;
    }

    public void setPayerDfspId(String payerDfspId) {
        this.payerDfspId = payerDfspId;
    }

    public void setPayerPartyId(String payerPartyId) {
        this.payerPartyId = payerPartyId;
    }

    public void setPayerPartyIdType(String payerPartyIdType) {
        this.payerPartyIdType = payerPartyIdType;
    }

    public void setPayerFee(BigDecimal payerFee) {
        this.payerFee = payerFee;
    }

    public void setPayerFeeCurrency(String payerFeeCurrency) {
        this.payerFeeCurrency = payerFeeCurrency;
    }

    public void setPayerQuoteCode(String payerQuoteCode) {
        this.payerQuoteCode = payerQuoteCode;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public void setDirection(String direction) {
        this.direction = direction;
    }

    public void setErrorInformation(String errorInformation) {
        this.errorInformation = errorInformation;
    }

    public void setBatchId(String batchId) {
        this.batchId = batchId;
    }

    public void setClientCorrelationId(String clientCorrelationId) {
        this.clientCorrelationId = clientCorrelationId;
    }
}
