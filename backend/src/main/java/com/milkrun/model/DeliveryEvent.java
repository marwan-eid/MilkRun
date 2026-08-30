package com.milkrun.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.UUID;

/**
 * Delivery event received from a van via Kafka.
 */
public record DeliveryEvent(
        @JsonProperty("event_id") UUID eventId,
        @JsonProperty("van_id") String vanId,
        @JsonProperty("route_id") String routeId,
        @JsonProperty("stop_index") int stopIndex,
        @JsonProperty("customer_id") String customerId,
        @JsonProperty("event_type") DeliveryEventType eventType,
        @JsonProperty("timestamp") Instant timestamp,
        @JsonProperty("location") Location location,
        @JsonProperty("parcels_delivered") int parcelsDelivered,
        @JsonProperty("delivery_duration_seconds") int deliveryDurationSeconds,
        @JsonProperty("sla_deadline") Instant slaDeadline,
        @JsonProperty("total_stops") int totalStops,
        @JsonProperty("notes") String notes) {
}
