package com.milkrun.fleet;

import com.milkrun.model.VanState;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;

/**
 * The whole fleet's current state, as seen by this backend instance.
 *
 * With one instance, that is simply what its own pipeline computed
 * ({@code milkrun.fanout.mode=local}). With several, each instance only
 * processes the vans on its Kafka partitions, so states are shared through a
 * Kafka topic and every instance keeps a replica of all of them
 * ({@code kafka}). SSE, the REST snapshot, dispatch and the analytics live
 * table all read from here.
 */
public interface FleetView {

    Collection<VanState> all();

    Optional<VanState> get(String vanId);

    /** Every state update, as it arrives. */
    Flux<VanState> updates();

    /** Updates sampled to at most one per van per interval (for SSE clients). */
    default Flux<VanState> sampledUpdates(Duration interval) {
        return updates()
                .groupBy(VanState::vanId)
                .flatMap(group -> group.sample(interval));
    }
}
