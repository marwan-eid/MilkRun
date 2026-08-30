package com.milkrun.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

/**
 * The materialized state of a van, pushed to the frontend via SSE.
 * Combines latest GPS data with computed ETA and SLA risk info.
 */
public record VanState(
    @JsonProperty("van_id") String vanId,
    @JsonProperty("route_id") String routeId,
    @JsonProperty("location") Location location,
    @JsonProperty("speed_kmh") double speedKmh,
    @JsonProperty("heading_degrees") double headingDegrees,
    @JsonProperty("battery_pct") int batteryPct,
    @JsonProperty("status") VanStatus status,
    @JsonProperty("current_stop_index") int currentStopIndex,
    @JsonProperty("total_stops") int totalStops,
    @JsonProperty("eta_next_stop_seconds") long etaNextStopSeconds,
    @JsonProperty("sla_risk") SlaRisk slaRisk,
    @JsonProperty("confidence") DataConfidence confidence,
    @JsonProperty("in_geofence") boolean inGeofence,
    @JsonProperty("geofence_name") String geofenceName,
    @JsonProperty("last_updated") Instant lastUpdated
) {

    public enum SlaRisk {
        NONE, WARNING, CRITICAL
    }

    public enum DataConfidence {
        REAL_TIME, INTERPOLATED, STALE
    }
}
