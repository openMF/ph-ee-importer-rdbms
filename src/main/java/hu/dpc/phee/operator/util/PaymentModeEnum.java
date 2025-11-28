package hu.dpc.phee.operator.util;

public enum PaymentModeEnum {
    CLOSED_LOOP("closedloop");
    private final String value;

    PaymentModeEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }
}
