package com.milkrun.api;

import com.milkrun.calcite.AnalyticsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

/**
 * Analytics over the Apache Calcite federation layer.
 *
 * Historical endpoints query PostgreSQL through Calcite; the live endpoints
 * join the in-memory fleet state with PostgreSQL in one SQL statement. A
 * failed query returns 503 with an error message instead of an empty list.
 */
@RestController
@RequestMapping("/api/analytics")
public class AnalyticsController {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsController.class);

    private final AnalyticsService analyticsService;

    public AnalyticsController(AnalyticsService analyticsService) {
        this.analyticsService = analyticsService;
    }

    /** Top delay zones ranked by total SLA breach seconds, e.g. {@code ?limit=10}. */
    @GetMapping("/delay-zones")
    public Mono<ResponseEntity<Object>> getDelayZones(@RequestParam(defaultValue = "10") int limit) {
        return respond("delay-zones", analyticsService.getTopDelayZones(limit));
    }

    /** SLA breaches grouped by severity and cause. */
    @GetMapping("/sla-summary")
    public Mono<ResponseEntity<Object>> getSlaSummary() {
        return respond("sla-summary", analyticsService.getSlaSummary());
    }

    /** Van rankings by success rate over completed routes, e.g. {@code ?limit=20}. */
    @GetMapping("/van-performance")
    public Mono<ResponseEntity<Object>> getVanPerformance(@RequestParam(defaultValue = "20") int limit) {
        return respond("van-performance", analyticsService.getVanPerformance(limit));
    }

    /** Dead-letter entries by reason. */
    @GetMapping("/dlq-summary")
    public Mono<ResponseEntity<Object>> getDlqSummary() {
        return respond("dlq-summary", analyticsService.getDlqSummary());
    }

    /** Every zone with its breach count and average delay. */
    @GetMapping("/heatmap")
    public Mono<ResponseEntity<Object>> getHeatmapData() {
        return respond("heatmap", analyticsService.getGeofenceHeatmapData());
    }

    /** Federated: vans inside delay zones right now, with each zone's breach history. */
    @GetMapping("/live-zone-risk")
    public Mono<ResponseEntity<Object>> getLiveZoneRisk() {
        return respond("live-zone-risk", analyticsService.getLiveZoneRisk());
    }

    /** Federated: vans at SLA risk right now, with their breach history. */
    @GetMapping("/at-risk-vans")
    public Mono<ResponseEntity<Object>> getAtRiskVans() {
        return respond("at-risk-vans", analyticsService.getAtRiskVans());
    }

    @GetMapping("/status")
    public Map<String, Object> getStatus() {
        return Map.of(
                "calciteReady", analyticsService.isReady(),
                "engine", "Apache Calcite " + java.util.Objects.requireNonNullElse(
                        org.apache.calcite.jdbc.Driver.class.getPackage().getImplementationVersion(), ""),
                "sources", List.of("milkrun: PostgreSQL (JDBC adapter)", "live: in-memory fleet state"));
    }

    private Mono<ResponseEntity<Object>> respond(String name, Mono<List<Map<String, Object>>> rows) {
        return rows
                .map(r -> ResponseEntity.ok().<Object>body(r))
                .onErrorResume(e -> {
                    log.warn("Analytics query {} failed: {}", name, e.toString());
                    return Mono.just(ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                            .body(Map.of("error", "Analytics query failed", "query", name)));
                });
    }
}
