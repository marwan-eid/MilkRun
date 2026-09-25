package com.milkrun.calcite;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Analytical queries through the Calcite federation layer.
 *
 * Calcite executes over JDBC, which blocks, so every query runs on a dedicated
 * single thread instead of the WebFlux event loop that also serves the live
 * SSE stream. Results are cached briefly, so a burst of dashboard viewers
 * costs one query, not one each.
 *
 * Tables are addressed as {@code milkrun.<table>} (PostgreSQL) and
 * {@code live.vans} (in-memory fleet state). Filters, joins and aggregates
 * over PostgreSQL tables are pushed down to PostgreSQL; joins with
 * {@code live.vans} are executed by Calcite.
 */
@Service
public class AnalyticsService {

    private final CalciteSchemaFactory calcite;
    private final Duration cacheTtl;
    private final Duration timeout;
    private final Scheduler scheduler = Schedulers.newSingle("calcite");
    private final Map<String, Cached> cache = new ConcurrentHashMap<>();

    private record Cached(long at, List<Map<String, Object>> rows) {
    }

    public AnalyticsService(CalciteSchemaFactory calcite,
            @Value("${milkrun.analytics.cache-ttl:10s}") Duration cacheTtl,
            @Value("${milkrun.analytics.timeout:10s}") Duration timeout) {
        this.calcite = calcite;
        this.cacheTtl = cacheTtl;
        this.timeout = timeout;
    }

    @PreDestroy
    void shutdown() {
        scheduler.dispose();
    }

    // ═══════════════════ Historical (PostgreSQL) ═══════════════════

    /** Delay zones ranked by total SLA breach time. */
    public Mono<List<Map<String, Object>>> getTopDelayZones(int limit) {
        return query("delay-zones:" + limit, """
                SELECT g.name AS zone_name,
                       g.zone_type AS zone_type,
                       g.speed_factor AS speed_factor,
                       COUNT(*) AS breach_count,
                       SUM(s.breach_seconds) AS total_breach_seconds,
                       AVG(CAST(s.breach_seconds AS DOUBLE)) AS avg_breach_seconds
                FROM milkrun.sla_breaches s
                JOIN milkrun.geofence_zones g ON s.geofence_id = g.id
                WHERE g.active = TRUE
                GROUP BY g.name, g.zone_type, g.speed_factor
                ORDER BY total_breach_seconds DESC
                FETCH FIRST %d ROWS ONLY
                """.formatted(clampLimit(limit)));
    }

    /** SLA breaches grouped by severity and cause. */
    public Mono<List<Map<String, Object>>> getSlaSummary() {
        return query("sla-summary", """
                SELECT severity,
                       cause,
                       COUNT(*) AS breach_count,
                       AVG(CAST(breach_seconds AS DOUBLE)) AS avg_breach_seconds,
                       MAX(breach_seconds) AS max_breach_seconds
                FROM milkrun.sla_breaches
                GROUP BY severity, cause
                ORDER BY breach_count DESC
                """);
    }

    /** Vans ranked by delivery success rate over their completed routes. */
    public Mono<List<Map<String, Object>>> getVanPerformance(int limit) {
        return query("van-performance:" + limit, """
                SELECT van_id,
                       COUNT(*) AS total_routes,
                       SUM(completed_stops) AS total_completed,
                       SUM(failed_stops) AS total_failed,
                       CASE WHEN SUM(completed_stops) + SUM(failed_stops) > 0
                            THEN CAST(SUM(completed_stops) AS DOUBLE) * 100
                                 / (SUM(completed_stops) + SUM(failed_stops))
                            ELSE 0.0 END AS success_rate_pct,
                       AVG(CAST(avg_speed_kmh AS DOUBLE)) AS avg_speed,
                       AVG(CAST(total_distance_km AS DOUBLE)) AS avg_distance_km
                FROM milkrun.completed_routes
                WHERE status = 'COMPLETED'
                GROUP BY van_id
                ORDER BY success_rate_pct DESC, total_routes DESC
                FETCH FIRST %d ROWS ONLY
                """.formatted(clampLimit(limit)));
    }

    /** Dead-letter entries by reason and reconciliation state. */
    public Mono<List<Map<String, Object>>> getDlqSummary() {
        return query("dlq-summary", """
                SELECT error_reason,
                       COUNT(*) AS event_count,
                       SUM(CASE WHEN reconciled THEN 1 ELSE 0 END) AS reconciled_count,
                       SUM(CASE WHEN reconciled THEN 0 ELSE 1 END) AS pending_count
                FROM milkrun.dead_letter_log
                GROUP BY error_reason
                ORDER BY event_count DESC
                """);
    }

    /** Every active zone with its breach count and average delay (zero when none). */
    public Mono<List<Map<String, Object>>> getGeofenceHeatmapData() {
        return query("heatmap", """
                SELECT g.name AS zone_name,
                       g.zone_type AS zone_type,
                       g.speed_factor AS speed_factor,
                       COUNT(s.breach_seconds) AS hit_count,
                       COALESCE(AVG(CAST(s.breach_seconds AS DOUBLE)), 0.0) AS avg_delay_seconds
                FROM milkrun.geofence_zones g
                LEFT JOIN milkrun.sla_breaches s ON s.geofence_id = g.id
                WHERE g.active = TRUE
                GROUP BY g.name, g.zone_type, g.speed_factor
                ORDER BY hit_count DESC
                """);
    }

    // ═══════════════════ Federated (live fleet + PostgreSQL) ═══════════════════

    /**
     * Vans currently inside a delay zone, with that zone's breach history:
     * which vans are in the places that have historically made deliveries late.
     */
    public Mono<List<Map<String, Object>>> getLiveZoneRisk() {
        return query("live-zone-risk", """
                SELECT v.van_id,
                       v.status,
                       v.zone_name,
                       v.sla_risk,
                       v.slack_seconds,
                       z.speed_factor,
                       COALESCE(h.breach_count, 0) AS zone_breaches,
                       h.avg_breach_seconds AS zone_avg_breach_seconds
                FROM live.vans v
                JOIN milkrun.geofence_zones z ON z.name = v.zone_name
                LEFT JOIN (
                    SELECT g.name AS zone,
                           COUNT(*) AS breach_count,
                           AVG(CAST(s.breach_seconds AS DOUBLE)) AS avg_breach_seconds
                    FROM milkrun.sla_breaches s
                    JOIN milkrun.geofence_zones g ON s.geofence_id = g.id
                    GROUP BY g.name
                ) h ON h.zone = v.zone_name
                WHERE v.in_zone
                ORDER BY zone_breaches DESC, v.van_id
                """);
    }

    /**
     * Vans whose next stop is at risk right now, with how often each has been
     * late before.
     */
    public Mono<List<Map<String, Object>>> getAtRiskVans() {
        return query("at-risk-vans", """
                SELECT v.van_id,
                       v.status,
                       v.sla_risk,
                       v.slack_seconds,
                       v.eta_seconds,
                       v.zone_name,
                       COALESCE(h.breaches, 0) AS past_breaches,
                       h.avg_breach_seconds AS past_avg_breach_seconds
                FROM live.vans v
                LEFT JOIN (
                    SELECT van_id,
                           COUNT(*) AS breaches,
                           AVG(CAST(breach_seconds AS DOUBLE)) AS avg_breach_seconds
                    FROM milkrun.sla_breaches
                    GROUP BY van_id
                ) h ON h.van_id = v.van_id
                WHERE v.sla_risk IN ('WARNING', 'CRITICAL')
                ORDER BY v.slack_seconds, v.van_id
                """);
    }

    public boolean isReady() {
        return calcite.isReady();
    }

    // ═══════════════════ Execution ═══════════════════

    private Mono<List<Map<String, Object>>> query(String key, String sql) {
        return Mono.defer(() -> {
            Cached hit = cache.get(key);
            if (hit != null && System.currentTimeMillis() - hit.at() < cacheTtl.toMillis()) {
                return Mono.just(hit.rows());
            }
            return Mono.fromCallable(() -> calcite.executeQuery(sql))
                    .subscribeOn(scheduler)
                    .timeout(timeout)
                    .doOnNext(rows -> cache.put(key, new Cached(System.currentTimeMillis(), rows)));
        });
    }

    private static int clampLimit(int limit) {
        return Math.max(1, Math.min(limit, 100));
    }
}
