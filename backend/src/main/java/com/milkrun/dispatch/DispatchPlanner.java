package com.milkrun.dispatch;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.milkrun.model.Location;

import java.time.Instant;
import java.util.Optional;

/**
 * Decides which van takes an ad-hoc order and where in its route the stop goes.
 */
public interface DispatchPlanner {

    Optional<Assignment> assign(Order order);

    record Order(String orderId, Location location, Instant requestedAt) {
    }

    /**
     * The chosen insertion. Also the message published to the dispatch-events
     * topic (keyed by van id), which the simulator applies to that van.
     */
    record Assignment(
            @JsonProperty("order_id") String orderId,
            @JsonProperty("van_id") String vanId,
            @JsonProperty("route_id") String routeId,
            @JsonProperty("latitude") double latitude,
            @JsonProperty("longitude") double longitude,
            /** Insert before this stop index (== stop count: after the last stop). */
            @JsonProperty("insert_before") int insertBefore,
            @JsonProperty("stops_after_insert") int stopsAfterInsert,
            /** Extra driving distance the detour adds, km. */
            @JsonProperty("extra_km") double extraKm,
            @JsonProperty("predicted_arrival") Instant predictedArrival,
            @JsonProperty("sla_deadline") Instant slaDeadline,
            /** Stops on the van's route that the detour pushes past their deadline. */
            @JsonProperty("stops_made_late") int stopsMadeLate,
            /** Whether the new stop itself is predicted to make its slot. */
            @JsonProperty("on_time") boolean onTime,
            /** Van/position combinations evaluated. */
            @JsonProperty("options_considered") int optionsConsidered,
            @JsonProperty("created_at") Instant createdAt) {
    }
}
