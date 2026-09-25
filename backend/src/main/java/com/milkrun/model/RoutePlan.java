package com.milkrun.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.List;

/**
 * A van's planned route, published by the simulator to the compacted
 * route-plans topic whenever a route starts or changes.
 *
 * Speeds are in simulated km/h; {@code timeScale} is simulated seconds per
 * wall-clock second, so a van covers ground timeScale times faster than its
 * reported speed. Timestamps (deadlines, planned arrivals) are wall-clock.
 */
public record RoutePlan(
        @JsonProperty("route_id") String routeId,
        @JsonProperty("van_id") String vanId,
        @JsonProperty("version") int version,
        @JsonProperty("created_at") Instant createdAt,
        @JsonProperty("time_scale") double timeScale,
        @JsonProperty("base_speed_kmh") double baseSpeedKmh,
        @JsonProperty("stops") List<Stop> stops,
        /** Route polyline as [latitude, longitude] pairs. */
        @JsonProperty("waypoints") double[][] waypoints) {

    public record Stop(
            @JsonProperty("stop_index") int stopIndex,
            @JsonProperty("customer_id") String customerId,
            @JsonProperty("location") Location location,
            @JsonProperty("sla_deadline") Instant slaDeadline,
            @JsonProperty("planned_arrival") Instant plannedArrival,
            @JsonProperty("parcels") int parcels,
            /** Index of the polyline vertex where the van stops. */
            @JsonProperty("waypoint_index") int waypointIndex) {
    }

    /**
     * @return null if the plan is usable, otherwise why not.
     */
    public String validationError() {
        if (routeId == null || vanId == null) return "missing route_id or van_id";
        if (waypoints == null || waypoints.length < 2) return "fewer than 2 waypoints";
        for (double[] w : waypoints) {
            if (w == null || w.length != 2) return "malformed waypoint";
        }
        if (!(timeScale > 0) || !(baseSpeedKmh > 0)) return "non-positive time_scale or base_speed_kmh";
        if (stops == null) return "missing stops";
        int previous = 0;
        for (Stop s : stops) {
            if (s == null || s.slaDeadline() == null || s.customerId() == null) return "incomplete stop";
            if (s.waypointIndex() < previous || s.waypointIndex() >= waypoints.length) {
                return "stop waypoint_index out of order or range";
            }
            previous = s.waypointIndex();
        }
        return null;
    }
}
