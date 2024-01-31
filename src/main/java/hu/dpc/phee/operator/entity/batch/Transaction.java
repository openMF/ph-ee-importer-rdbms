package hu.dpc.phee.operator.entity.batch;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({ "id", "request_id", "payment_mode", "account_number", "payer_identifier_type", "payer_identifier",
        "payee_identifier_type", "payee_identifier", "amount", "currency", "note", "program_shortcode", "cycle" , "batch_id"})
public class Transaction implements CsvSchema {

    @JsonProperty("id")
    private int id;

    @JsonProperty("request_id")
    private String requestId;

    @JsonProperty("payment_mode")
    private String paymentMode;

    @JsonProperty("account_number")
    private String accountNumber;

    @JsonProperty("amount")
    private String amount;

    @JsonProperty("currency")
    private String currency;

    @JsonProperty("note")
    private String note;

    @JsonProperty(value = "payer_identifier_type")
    private String payerIdentifierType;

    @JsonProperty("payer_identifier")
    private String payerIdentifier;

    @JsonProperty("payee_identifier_type")
    private String payeeIdentifierType;

    @JsonProperty("payee_identifier")
    private String payeeIdentifier;

    @JsonProperty("program_shortcode")
    private String programShortCode;

    @JsonProperty("cycle")
    private String cycle;

    @JsonProperty("batch_id")
    private String batchId;

    @JsonIgnore
    private String status;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getPaymentMode() {
        return paymentMode;
    }

    public void setPaymentMode(String paymentMode) {
        this.paymentMode = paymentMode;
    }

    public String getAccountNumber() {
        return accountNumber;
    }

    public void setAccountNumber(String accountNumber) {
        this.accountNumber = accountNumber;
    }

    public String getAmount() {
        return amount;
    }

    public void setAmount(String amount) {
        this.amount = amount;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getNote() {
        return note;
    }

    public void setNote(String note) {
        this.note = note;
    }

    public String getBatchId() {
        return batchId;
    }

    public void setBatchId(String batchId) {
        this.batchId = batchId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getPayerIdentifierType() {
        return payerIdentifierType;
    }

    public void setPayerIdentifierType(String payerIdentifierType) {
        this.payerIdentifierType = payerIdentifierType;
    }

    public String getPayerIdentifier() {
        return payerIdentifier;
    }

    public void setPayerIdentifier(String payerIdentifier) {
        this.payerIdentifier = payerIdentifier;
    }

    public String getPayeeIdentifierType() {
        return payeeIdentifierType;
    }

    public void setPayeeIdentifierType(String payeeIdentifierType) {
        this.payeeIdentifierType = payeeIdentifierType;
    }

    public String getPayeeIdentifier() {
        return payeeIdentifier;
    }

    public void setPayeeIdentifier(String payeeIdentifier) {
        this.payeeIdentifier = payeeIdentifier;
    }

    public String getProgramShortCode() {
        return programShortCode;
    }

    public void setProgramShortCode(String programShortCode) {
        this.programShortCode = programShortCode;
    }

    public String getCycle() {
        return cycle;
    }

    public void setCycle(String cycle) {
        this.cycle = cycle;
    }

    @Override
    public String toString() {
        return "Transaction{" + "id=" + id + ", request_id='" + requestId + '\'' + ", payment_mode='" + paymentMode + '\''
                + ", account_number='" + accountNumber + '\'' + ", amount='" + amount + '\'' + ", currency='" + currency + '\'' + ", note='"
                + note + '\'' + ", batchId='" + batchId + '\'' + ", status='" + status + '\'' + '}';
    }

    @JsonIgnore
    @Override
    public String getCsvString() {
        if (status == null || !status.isEmpty()) {
            return String.format("%s,%s,%s,%s,%s,%s,%s", id, requestId, paymentMode, accountNumber, amount, currency, note);
        }
        return String.format("%s,%s,%s,%s,%s,%s,%s,%s", id, requestId, paymentMode, accountNumber, amount, currency, note, status);
    }

    @JsonIgnore
    @Override
    public String getCsvHeader() {
        if (status.isEmpty()) {
            return "id,request_id,payment_mode,account_number,amount,currency,note";
        }
        return "id,request_id,payment_mode,account_number,amount,currency,note,status";
    }
}
