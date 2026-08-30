package com.milkrun.model;

import java.time.Instant;
import java.util.UUID;

public record DispatchEvent(
        String eventId,
        double latitude,
        double longitude,
        Instant timestamp) {
    public DispatchEvent(double latitude, double longitude) {
        this(UUID.randomUUID().toString(), latitude, longitude, Instant.now());
    }
}
