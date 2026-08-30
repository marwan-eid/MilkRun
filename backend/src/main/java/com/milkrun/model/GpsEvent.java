package com.milkrun.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.UUID;

/**
 * GPS event received from a delivery van via Kafka.
 * Matches the TypeScript simulator's GpsEvent schema exactly.
 */
public record GpsEvent(
    @JsonProperty("event_id") UUID eventId,
    @JsonProperty("van_id") String vanId,
    @JsonProperty("sequence_number") long sequenceNumber,
    @JsonProperty("device_timestamp") Instant deviceTimestamp,
    @JsonProperty("ingestion_timestamp") Instant ingestionTimestamp,
    @JsonProperty("location") Location location,
    @JsonProperty("speed_kmh") double speedKmh,
    @JsonProperty("heading_degrees") double headingDegrees,
    @JsonProperty("battery_pct") int batteryPct,
    @JsonProperty("route_id") String routeId,
    @JsonProperty("current_stop_index") int currentStopIndex,
    @JsonProperty("total_stops") int totalStops,
    @JsonProperty("status") VanStatus status
) {}
