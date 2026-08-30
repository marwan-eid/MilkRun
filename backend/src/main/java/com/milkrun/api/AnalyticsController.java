package com.milkrun.api;

import com.milkrun.calcite.AnalyticsService;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * REST controller for analytics endpoints powered by Apache Calcite.
 *
 * All endpoints return data queried through Calcite's federated layer
 * rather than directly through R2DBC, demonstrating the separation of
 * real-time (R2DBC/reactive) vs. analytical (Calcite/JDBC) query paths.
 */
@RestController
@RequestMapping("/api/analytics")
@CrossOrigin(origins = { "http://localhost:5173", "http://localhost:3000" })
public class AnalyticsController {

    private final AnalyticsService analyticsService;

    public AnalyticsController(AnalyticsService analyticsService) {
        this.analyticsService = analyticsService;
    }

    /**
     * Top delay zones ranked by total SLA breach seconds.
     * Used by the frontend heatmap overlay.
     *
     * Example: GET /api/analytics/delay-zones?limit=10
     */
    @GetMapping("/delay-zones")
    public List<Map<String, Object>> getDelayZones(
            @RequestParam(defaultValue = "10") int limit) {
        return analyticsService.getTopDelayZones(limit);
    }

    /**
     * SLA breach summary grouped by severity and cause.
     *
     * Example: GET /api/analytics/sla-summary
     */
    @GetMapping("/sla-summary")
    public List<Map<String, Object>> getSlaSummary() {
        return analyticsService.getSlaSummary();
    }

    /**
     * Van performance rankings by success rate and route count.
     *
     * Example: GET /api/analytics/van-performance?limit=20
     */
    @GetMapping("/van-performance")
    public List<Map<String, Object>> getVanPerformance(
            @RequestParam(defaultValue = "20") int limit) {
        return analyticsService.getVanPerformance(limit);
    }

    /**
     * Dead letter queue event summary by error reason.
     *
     * Example: GET /api/analytics/dlq-summary
     */
    @GetMapping("/dlq-summary")
    public List<Map<String, Object>> getDlqSummary() {
        return analyticsService.getDlqSummary();
    }

    /**
     * Geofence heatmap data with delay severity for map overlay.
     *
     * Example: GET /api/analytics/heatmap
     */
    @GetMapping("/heatmap")
    public List<Map<String, Object>> getHeatmapData() {
        return analyticsService.getGeofenceHeatmapData();
    }

    /**
     * Calcite ready check.
     */
    @GetMapping("/status")
    public Map<String, Object> getStatus() {
        return Map.of(
                "calciteReady", analyticsService.isReady(),
                "engine", "Apache Calcite 1.37.0",
                "federation", "PostgreSQL JDBC adapter");
    }
}
