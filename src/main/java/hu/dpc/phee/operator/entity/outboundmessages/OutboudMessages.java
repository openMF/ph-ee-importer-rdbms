package hu.dpc.phee.operator.entity.outboundmessages;

import hu.dpc.phee.operator.entity.parent.AbstractPersistableCustom;

import hu.dpc.phee.operator.entity.transfer.TransferStatus;
import hu.dpc.phee.operator.util.SmsMessageStatusType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import jakarta.persistence.Temporal;
import jakarta.persistence.TemporalType;
import lombok.Data;

import java.util.Date;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

@Entity
@Table(name = "m_outbound_messages")
public class OutboudMessages extends AbstractPersistableCustom<Long> {

    @com.fasterxml.jackson.annotation.JsonIgnore
    @Column(name = "tenant_id", nullable = false)
    private Long tenantId;

    @Column(name = "external_id", nullable = true)
    private String externalId;

    @Column(name = "internal_id", nullable = false)
    private Long internalId;

    @Column(name = "submitted_on_date", nullable = true)
    @Temporal(TemporalType.DATE)
    private Date submittedOnDate;

    @Column(name = "delivered_on_date", nullable = true)
    @Temporal(TemporalType.TIMESTAMP)
    private Date deliveredOnDate;

    @Column(name = "delivery_status", nullable = false)
    private Integer deliveryStatus = SmsMessageStatusType.PENDING.getValue();

    @Column(name = "delivery_error_message", nullable = true)
    private String deliveryErrorMessage;

    @Column(name = "source_address", nullable = true)
    private String sourceAddress;

    @Column(name = "mobile_number", nullable = false)
    private String mobileNumber;

    @Column(name = "message", nullable = false)
    private String message;

    @Column(name = "sms_bridge_id", nullable = false)
    private Long bridgeId;

    public OutboudMessages() {}

    public OutboudMessages(Long workflowInstanceKey) {
        this.internalId = workflowInstanceKey;
        this.deliveryStatus = SmsMessageStatusType.PENDING.getValue();
    }

    public Long getTenantId() {
        return tenantId;
    }

    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    public String getExternalId() {
        return externalId;
    }

    public void setExternalId(String externalId) {
        this.externalId = externalId;
    }

    public Long getInternalId() {
        return internalId;
    }

    public void setInternalId(Long internalId) {
        this.internalId = internalId;
    }

    public Date getSubmittedOnDate() {
        return submittedOnDate;
    }

    public void setSubmittedOnDate(Date submittedOnDate) {
        this.submittedOnDate = submittedOnDate;
    }

    public Date getDeliveredOnDate() {
        return deliveredOnDate;
    }

    public void setDeliveredOnDate(Date deliveredOnDate) {
        this.deliveredOnDate = deliveredOnDate;
    }

    public Integer getDeliveryStatus() {
        return deliveryStatus;
    }

    public void setDeliveryStatus(Integer deliveryStatus) {
        this.deliveryStatus = deliveryStatus;
    }

    public String getDeliveryErrorMessage() {
        return deliveryErrorMessage;
    }

    public void setDeliveryErrorMessage(String deliveryErrorMessage) {
        this.deliveryErrorMessage = deliveryErrorMessage;
    }

    public String getSourceAddress() {
        return sourceAddress;
    }

    public void setSourceAddress(String sourceAddress) {
        this.sourceAddress = sourceAddress;
    }

    public String getMobileNumber() {
        return mobileNumber;
    }

    public void setMobileNumber(String mobileNumber) {
        this.mobileNumber = mobileNumber;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Long getBridgeId() {
        return bridgeId;
    }

    public void setBridgeId(Long bridgeId) {
        this.bridgeId = bridgeId;
    }

    public String getResponse() {
        return response;
    }

    public void setResponse(String response) {
        this.response = response;
    }

    @Column(name = "response")
    private String response;

}
