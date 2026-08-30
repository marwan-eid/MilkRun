package com.milkrun.api;

import com.milkrun.consumer.GpsEventPipeline;
import com.milkrun.engine.EtaEngine;
import com.milkrun.model.VanState;
import com.milkrun.pipeline.BloomFilterDedup;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;

/**
 * REST + SSE controller for the Milk-Run frontend.
 *
 * Endpoints:
 * - GET /api/stream/vans          → SSE stream of VanState updates
 * - GET /api/vans                 → Current state of all vans (snapshot)
 * - GET /api/vans/{vanId}         → Current state of a specific van
 * - GET /api/health/pipeline      → Pipeline health metrics
 */
@RestController
@RequestMapping("/api")
@CrossOrigin(origins = {"http://localhost:5173", "http://localhost:3000"})
public class VanStreamController {

    private final GpsEventPipeline pipeline;
    private final EtaEngine etaEngine;
    private final BloomFilterDedup dedup;

    public VanStreamController(GpsEventPipeline pipeline, EtaEngine etaEngine, BloomFilterDedup dedup) {
        this.pipeline = pipeline;
        this.etaEngine = etaEngine;
        this.dedup = dedup;
    }

    /**
     * Server-Sent Events stream of van state updates.
     * Backpressure-sampled: max 2 updates/sec per van.
     *
     * Frontend connects via EventSource API:
     *   const source = new EventSource('/api/stream/vans');
     *   source.onmessage = (e) => updateMap(JSON.parse(e.data));
     */
    @GetMapping(value = "/stream/vans", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<VanState>> streamVanUpdates() {
        return pipeline.vanStateStream()
                .map(state -> ServerSentEvent.<VanState>builder()
                        .id(state.vanId() + "-" + state.lastUpdated().toEpochMilli())
                        .event("van-update")
                        .data(state)
                        .build())
                // Heartbeat every 15 seconds to keep connection alive
                .mergeWith(
                        Flux.interval(Duration.ofSeconds(15))
                                .map(tick -> ServerSentEvent.<VanState>builder()
                                        .event("heartbeat")
                                        .comment("keepalive")
                                        .build())
                );
    }

    /**
     * Get current snapshot of all van states.
     */
    @GetMapping("/vans")
    public Collection<VanState> getAllVans() {
        return etaEngine.getAllVanStates().values();
    }

    /**
     * Get current state of a specific van.
     */
    @GetMapping("/vans/{vanId}")
    public VanState getVan(@PathVariable String vanId) {
        VanState state = etaEngine.getAllVanStates().get(vanId);
        if (state == null) {
            throw new VanNotFoundException(vanId);
        }
        return state;
    }

    /**
     * Pipeline health and metrics endpoint.
     */
    @GetMapping("/health/pipeline")
    public Map<String, Object> pipelineHealth() {
        return Map.of(
                "activeVans", etaEngine.getAllVanStates().size(),
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
