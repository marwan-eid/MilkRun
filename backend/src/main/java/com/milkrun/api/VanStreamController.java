package com.milkrun.api;

import com.milkrun.fleet.FleetView;
import com.milkrun.model.VanState;
import com.milkrun.pipeline.Deduplicator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;

/**
 * REST + SSE endpoints for the dashboard.
 *
 * - GET /api/stream/vans          SSE stream of van state updates
 * - GET /api/vans                 current state of all vans (snapshot)
 * - GET /api/vans/{vanId}         current state of one van
 * - GET /api/health/pipeline      pipeline counters
 *
 * Reads the {@link FleetView}, so with several backend instances each one
 * serves the whole fleet.
 */
@RestController
@RequestMapping("/api")
public class VanStreamController {

    private final FleetView fleet;
    private final Deduplicator dedup;
    private final Duration sampleInterval;

    public VanStreamController(FleetView fleet, Deduplicator dedup,
            @Value("${milkrun.backpressure.sample-interval-ms:500}") long sampleIntervalMs) {
        this.fleet = fleet;
        this.dedup = dedup;
        this.sampleInterval = Duration.ofMillis(sampleIntervalMs);
    }

    /**
     * Server-Sent Events stream of van state updates, at most one per van per
     * sample interval, with a heartbeat every 15 seconds.
     *
     * Frontend connects via EventSource API:
     *   const source = new EventSource('/api/stream/vans');
     *   source.addEventListener('van-update', (e) => updateMap(JSON.parse(e.data)));
     */
    @GetMapping(value = "/stream/vans", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<VanState>> streamVanUpdates() {
        return fleet.sampledUpdates(sampleInterval)
                .map(state -> ServerSentEvent.<VanState>builder()
                        .id(state.vanId() + "-" + state.lastUpdated().toEpochMilli())
                        .event("van-update")
                        .data(state)
                        .build())
                .mergeWith(Flux.interval(Duration.ofSeconds(15))
                        .map(tick -> ServerSentEvent.<VanState>builder()
                                .event("heartbeat")
                                .comment("keepalive")
                                .build()));
    }

    @GetMapping("/vans")
    public Collection<VanState> getAllVans() {
        return fleet.all();
    }

    @GetMapping("/vans/{vanId}")
    public VanState getVan(@PathVariable String vanId) {
        return fleet.get(vanId).orElseThrow(() -> new VanNotFoundException(vanId));
    }

    @GetMapping("/health/pipeline")
    public Map<String, Object> pipelineHealth() {
        return Map.of(
                "activeVans", fleet.all().size(),
                "dedupChecked", dedup.getTotalChecked(),
                "dedupRejected", dedup.getDuplicatesRejected(),
                "status", "RUNNING"
        );
    }

    @ResponseStatus(org.springframework.http.HttpStatus.NOT_FOUND)
    static class VanNotFoundException extends RuntimeException {
        VanNotFoundException(String vanId) {
            super("Van not found: " + vanId);
        }
    }
}
