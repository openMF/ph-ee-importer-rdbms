package hu.dpc.phee.operator.entity.batch;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({ "id", "request_id", "payment_mode", "account_number", "payer_identifier_type", "payer_identifier", "payee_identifier_type", "payee_identifier", "amount", "currency", "note", "program_shortcode", "cycle" })
@Getter
@Setter
@ToString
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

    @JsonIgnore
    private String batchId;

    @JsonIgnore
    private String status;

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
