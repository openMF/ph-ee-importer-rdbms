package hu.dpc.phee.operator.business;


import org.eclipse.persistence.annotations.Index;

import javax.persistence.Cacheable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;
import java.util.Date;

@Entity
@Table(name = "transactions")
@Cacheable(false)
public class Transaction {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    /**
     * zeebe workflowInstanceKey
     */
    @Column(name = "WORKFLOW_INSTANCE_KEY")
    @Index(name = "idx_paymentProcessId")
    private Long workflowInstanceKey;

    private String transactionId;

    @Column(name = "STARTED_AT")
    private Date startedAt;
    @Column(name = "COMPLETED_AT")
    private Date completedAt;

    private String status; // TODO enum

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

    private BigDecimal amount;
    private String currency;


    Transaction() {
        // jpa constructor
    }

    public Transaction(Long workflowInstanceKey) {
        this.workflowInstanceKey = workflowInstanceKey;
    }


    public Date getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(Date completedAt) {
        this.completedAt = completedAt;
    }

    public Date getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(Date startedAt) {
        this.startedAt = startedAt;
    }

    public BigDecimal getPayeeFee() {
        return payeeFee;
    }

    public void setPayeeFee(BigDecimal payeeFee) {
        this.payeeFee = payeeFee;
    }

    public String getPayeeQuoteCode() {
        return payeeQuoteCode;
    }

    public void setPayeeQuoteCode(String payeeQuoteCode) {
        this.payeeQuoteCode = payeeQuoteCode;
    }

    public BigDecimal getPayerFee() {
        return payerFee;
    }

    public void setPayerFee(BigDecimal payerFee) {
        this.payerFee = payerFee;
    }

    public String getPayerQuoteCode() {
        return payerQuoteCode;
    }

    public void setPayerQuoteCode(String payerQuoteCode) {
        this.payerQuoteCode = payerQuoteCode;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public Long getWorkflowInstanceKey() {
        return workflowInstanceKey;
    }

    public void setWorkflowInstanceKey(Long paymentProcessId) {
        this.workflowInstanceKey = paymentProcessId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getPayeePartyId() {
        return payeePartyId;
    }

    public void setPayeePartyId(String payeePartyId) {
        this.payeePartyId = payeePartyId;
    }

    public String getPayeePartyIdType() {
        return payeePartyIdType;
    }

    public void setPayeePartyIdType(String payeePartyType) {
        this.payeePartyIdType = payeePartyType;
    }

    public String getPayerPartyId() {
        return payerPartyId;
    }

    public void setPayerPartyId(String payerPartyId) {
        this.payerPartyId = payerPartyId;
    }

    public String getPayerPartyIdType() {
        return payerPartyIdType;
    }

    public void setPayerPartyIdType(String payerPartyType) {
        this.payerPartyIdType = payerPartyType;
    }

    public String getPayeeFeeCurrency() {
        return payeeFeeCurrency;
    }

    public void setPayeeFeeCurrency(String payeeFeeCurrency) {
        this.payeeFeeCurrency = payeeFeeCurrency;
    }

    public String getPayerFeeCurrency() {
        return payerFeeCurrency;
    }

    public void setPayerFeeCurrency(String payerFeeCurrency) {
        this.payerFeeCurrency = payerFeeCurrency;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public String getPayeeDfspId() {
        return payeeDfspId;
    }

    public void setPayeeDfspId(String payeeDfspId) {
        this.payeeDfspId = payeeDfspId;
    }

    public String getPayerDfspId() {
        return payerDfspId;
    }

    public void setPayerDfspId(String payerDfspId) {
        this.payerDfspId = payerDfspId;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }
}
