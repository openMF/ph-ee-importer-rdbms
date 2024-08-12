package hu.dpc.phee.operator.event.parser.impl.card.auth.entity;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import lombok.Data;

import java.math.BigDecimal;

@Data
public class TransactionFeedRequest {

    public enum MsgType {
        AUTH,
        AUTH_REVERSAL,
        AUTH_DECLINE,
        FINANCIAL_NOTIFICATION,
    }

    private String requestId;
    private BigDecimal accId;
    private String msgId;
    private BigDecimal accAmount;
    private String accCurrencyCode;
    private BigDecimal trnAmount;
    private String trnCurrencyCode;
    private BigDecimal feeAmount;
    private BigDecimal settleAmount;
    private String settleCurrencyCode;
    private BigDecimal holdAmount;
    private BigDecimal attemptNo;
    private String eventDate;
    private String trnRef;
    private Boolean needApprove;
    private String mccCode;
    private String merchIdDE42;
    private String terminalId;
    private String posPanEntryMode;
    private String posPinEntryCapability;
    private String txnDesc;
    private MsgType msgType;
    private String procCode;
    private Boolean isEcommerce;
    private Boolean isContactless;
    private Boolean is3ds;
    private String dsTransId;
    private String paymentTokenWallet;
    private String authType;
    private String merchName;
    private String merchStreet;
    private String merchCity;
    private String merchRegion;
    private String merchPostcode;
    private String merchCountry;
    private String txnCtry;
    private String respCode;
    private String declineNote;
    private String token;

    private static final ObjectMapper objectMapper = JsonMapper.builder()
            .findAndAddModules()
            .configure(DeserializationFeature.USE_JAVA_ARRAY_FOR_JSON_ARRAY, true)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
            .configure(SerializationFeature.INDENT_OUTPUT, false)
            .build();

    public String toJson() {
        try {
            return objectMapper.writer().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("failed to create json text from TransactionFeedRequest", e);
        }
    }

    public static TransactionFeedRequest fromJson(String json) {
        try {
            return objectMapper.readValue(json, TransactionFeedRequest.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("failed to create TransactionFeedRequest object from json text", e);
        }
    }
}