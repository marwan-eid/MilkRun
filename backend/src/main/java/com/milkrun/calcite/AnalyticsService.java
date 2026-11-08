package com.milkrun.calcite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Analytical queries through the Calcite federation layer.
 *
 * Calcite executes over JDBC, which blocks, so every query runs on a dedicated
 * single thread instead of the WebFlux event loop that also serves the live
 * SSE stream.
 *
 * Callers are answered from the last result whenever it is recent enough, so
 * a visitor does not wait for a query on a slow machine. On the demo VM a
 * query can take tens of seconds and the whole JVM sometimes stalls for a
 * minute or more; querying on demand failed most dashboard opens there. So:
 * <ul>
 *   <li>a result younger than the cache TTL is served as it is;</li>
 *   <li>an older one, up to max-stale, is served too while a refresh runs;</li>
 *   <li>only without either does the caller wait, up to the timeout;</li>
 *   <li>the dashboard's queries are also refreshed on a schedule, so they
 *       always have a result to serve.</li>
 * </ul>
 * Every result carries the time it was computed, which the dashboard shows.
 *
 * Tables are addressed as {@code milkrun.<table>} (PostgreSQL) and
 * {@code live.vans} (in-memory fleet state). Filters, joins and aggregates
 * over PostgreSQL tables are pushed down to PostgreSQL; joins with
 * {@code live.vans} are executed by Calcite.
 */
@Service
public class AnalyticsService {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsService.class);

    /** The limits the dashboard's analytics panel asks for (frontend AnalyticsPanel.tsx). */
    static final int DASHBOARD_DELAY_ZONES = 5;
    static final int DASHBOARD_VANS = 10;

    /** Rows of a query and when they were computed. */
    public record Rows(Instant asOf, List<Map<String, Object>> rows) {
    }

    private record Query(String key, String sql) {
    }

    private final CalciteSchemaFactory calcite;
    private final Duration cacheTtl;
    private final Duration maxStale;
    private final Duration timeout;
    private final Duration refreshInterval;
    private final Scheduler scheduler = Schedulers.newSingle("calcite");
    private final Map<String, Rows> cache = new ConcurrentHashMap<>();
    private final Map<String, Mono<Rows>> inFlight = new ConcurrentHashMap<>();
    private Disposable refresher;

    @Autowired
    public AnalyticsService(CalciteSchemaFactory calcite,
            @Value("${milkrun.analytics.cache-ttl:10s}") Duration cacheTtl,
            @Value("${milkrun.analytics.max-stale:10m}") Duration maxStale,
            @Value("${milkrun.analytics.timeout:10s}") Duration timeout,
            @Value("${milkrun.analytics.refresh-interval:0s}") Duration refreshInterval) {
        this.calcite = calcite;
        this.cacheTtl = cacheTtl;
        this.maxStale = maxStale.compareTo(cacheTtl) > 0 ? maxStale : cacheTtl;
        this.timeout = timeout;
        this.refreshInterval = refreshInterval;
    }

    /** Without serving stale results or refreshing on a schedule. */
    public AnalyticsService(CalciteSchemaFactory calcite, Duration cacheTtl, Duration timeout) {
        this(calcite, cacheTtl, cacheTtl, timeout, Duration.ZERO);
    }

    /**
     * Refreshes the dashboard's queries on a schedule (when refresh-interval is
     * set), the first time shortly after startup, so the first visitor already
     * has results.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void startRefreshing() {
        if (refreshInterval.isZero() || refreshInterval.isNegative()) {
            return;
        }
        refresher = Flux.interval(Duration.ofMinutes(2), refreshInterval)
                // A refresh slower than the interval is not stacked up behind itself.
                .onBackpressureDrop()
                .concatMap(tick -> refreshDashboard(), 1)
                .subscribe(null, e -> log.error("Analytics refresher stopped", e));
    }

    @PreDestroy
    void shutdown() {
        if (refresher != null) {
            refresher.dispose();
        }
        scheduler.dispose();
    }

    /** Runs the dashboard's queries now, one after another; failures are logged. */
    Mono<Void> refreshDashboard() {
        if (!calcite.isReady()) {
            return Mono.empty();
        }
        return Flux.just(atRiskVans(), liveZoneRisk(), delayZones(DASHBOARD_DELAY_ZONES), vanPerformance(DASHBOARD_VANS))
                .concatMap(q -> run(q).onErrorResume(e -> {
                    log.warn("Refreshing analytics query {} failed: {}", q.key(), e.toString());
                    return Mono.empty();
                }))
                .then();
    }

    // ═══════════════════ Historical (PostgreSQL) ═══════════════════

    /** Delay zones ranked by total SLA breach time. */
    public Mono<Rows> getTopDelayZones(int limit) {
        return query(delayZones(limit));
    }

    private static Query delayZones(int limit) {
        int n = clampLimit(limit);
        return new Query("delay-zones:" + n, """
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
                """.formatted(n));
    }

    /** SLA breaches grouped by severity and cause. */
    public Mono<Rows> getSlaSummary() {
        return query(new Query("sla-summary", """
                SELECT severity,
                       cause,
                       COUNT(*) AS breach_count,
                       AVG(CAST(breach_seconds AS DOUBLE)) AS avg_breach_seconds,
                       MAX(breach_seconds) AS max_breach_seconds
                FROM milkrun.sla_breaches
                GROUP BY severity, cause
                ORDER BY breach_count DESC
                """));
    }

    /** Vans ranked by delivery success rate over their completed routes. */
    public Mono<Rows> getVanPerformance(int limit) {
        return query(vanPerformance(limit));
    }

    private static Query vanPerformance(int limit) {
        int n = clampLimit(limit);
        return new Query("van-performance:" + n, """
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
                """.formatted(n));
    }

    /** Dead-letter entries by reason and reconciliation state. */
    public Mono<Rows> getDlqSummary() {
        return query(new Query("dlq-summary", """
                SELECT error_reason,
                       COUNT(*) AS event_count,
                       SUM(CASE WHEN reconciled THEN 1 ELSE 0 END) AS reconciled_count,
                       SUM(CASE WHEN reconciled THEN 0 ELSE 1 END) AS pending_count
                FROM milkrun.dead_letter_log
                GROUP BY error_reason
                ORDER BY event_count DESC
                """));
    }

    /** Every active zone with its breach count and average delay (zero when none). */
    public Mono<Rows> getGeofenceHeatmapData() {
        return query(new Query("heatmap", """
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
                """));
    }

    // ═══════════════════ Federated (live fleet + PostgreSQL) ═══════════════════

    /**
     * Vans currently inside a delay zone, with that zone's breach history:
     * which vans are in the places that have historically made deliveries late.
     */
    public Mono<Rows> getLiveZoneRisk() {
        return query(liveZoneRisk());
    }

    private static Query liveZoneRisk() {
        return new Query("live-zone-risk", """
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
    public Mono<Rows> getAtRiskVans() {
        return query(atRiskVans());
    }

    private static Query atRiskVans() {
        return new Query("at-risk-vans", """
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

    /** Serves the last result if it is recent enough (see the class comment), else waits for a run. */
    private Mono<Rows> query(Query q) {
        return Mono.defer(() -> {
            Rows last = cache.get(q.key());
            Duration age = last != null ? Duration.between(last.asOf(), Instant.now()) : null;
            if (age != null && age.compareTo(cacheTtl) < 0) {
                return Mono.just(last);
            }
            if (age != null && age.compareTo(maxStale) < 0) {
                run(q).subscribe(null, e -> log.warn("Refreshing analytics query {} failed: {}", q.key(), e.toString()));
                return Mono.just(last);
            }
            return run(q).timeout(timeout);
        });
    }

    /**
     * Joins the run of this query that is already queued or running, or starts
     * one. A caller that stops waiting does not cancel the run: its result is
     * still cached for the next caller, and runs are bounded by PostgreSQL's
     * statement timeout instead (see {@link CalciteSchemaFactory}). Before, a
     * query slower than the timeout was discarded every time, and each
     * dashboard open queued more queries on the single Calcite thread behind
     * the abandoned ones.
     */
    private Mono<Rows> run(Query q) {
        return inFlight.computeIfAbsent(q.key(), key -> Mono.fromCallable(() -> {
                    Rows rows = new Rows(Instant.now(), calcite.executeQuery(q.sql()));
                    cache.put(key, rows);
                    return rows;
                })
                .subscribeOn(scheduler)
                .doFinally(signal -> inFlight.remove(key))
                // Shared by every caller; one caller cancelling leaves the run going
                .cache());
    }

    private static int clampLimit(int limit) {
        return Math.max(1, Math.min(limit, 100));
    }
}
