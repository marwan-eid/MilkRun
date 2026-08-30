package com.milkrun.model;

import com.fasterxml.jackson.annotation.JsonValue;

public enum VanStatus {
    EN_ROUTE, DELIVERING, IDLE, RETURNING, RETURNED;

    @JsonValue
    public String toValue() {
        return name();
    }
}
