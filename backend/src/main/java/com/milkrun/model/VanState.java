package com.milkrun.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

/**
 * The materialized state of a van, pushed to the frontend via SSE.
 * Combines the latest GPS data with the ETA and SLA risk for the next stop.
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
    /** Seconds until the van reaches its next stop; 0 while serving it; -1 if unknown or no next stop. */
    @JsonProperty("eta_next_stop_seconds") long etaNextStopSeconds,
    @JsonProperty("sla_risk") SlaRisk slaRisk,
    @JsonProperty("confidence") DataConfidence confidence,
    @JsonProperty("in_geofence") boolean inGeofence,
    @JsonProperty("geofence_name") String geofenceName,
    @JsonProperty("last_updated") Instant lastUpdated,
    /** Deadline of the next (or current) stop; null if unknown. */
    @JsonProperty("sla_deadline") Instant slaDeadline,
    /** Predicted (or, while delivering, actual) arrival at that stop; null if unknown. */
    @JsonProperty("predicted_arrival") Instant predictedArrival,
    /** Deadline minus predicted arrival in seconds (negative = late); null if unknown. */
    @JsonProperty("sla_slack_seconds") Long slaSlackSeconds,
    @JsonProperty("eta_method") EtaMethod etaMethod
) {

    public enum SlaRisk {
        /** Projected to arrive with more than the warning buffer to spare. */
        NONE,
        /** Projected to arrive, but with less than the warning buffer. */
        WARNING,
        /** Projected to arrive late (or arrived late). */
        CRITICAL,
        /** No route plan for this van yet, so no deadline to compare against. */
        UNKNOWN
    }

    public enum DataConfidence {
        REAL_TIME, INTERPOLATED, STALE
    }

    public enum EtaMethod {
        /** Distance along the planned route, zone-adjusted, at the van's measured speed. */
        ROUTE,
        /** Straight-line distance times a detour factor (van off its planned route, or estimator failing). */
        STRAIGHT_LINE,
        /** No next stop, or no plan. */
        NONE
    }
}
