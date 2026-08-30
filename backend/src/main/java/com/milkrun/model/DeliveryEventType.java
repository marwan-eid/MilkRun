package com.milkrun.model;

import com.fasterxml.jackson.annotation.JsonValue;

public enum DeliveryEventType {
    ARRIVAL, DELIVERY_COMPLETED, DELIVERY_FAILED, DEPARTURE;

    @JsonValue
    public String toValue() {
        return name();
    }
}
