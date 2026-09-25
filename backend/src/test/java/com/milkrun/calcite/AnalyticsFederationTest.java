package com.milkrun.calcite;

import com.milkrun.model.Location;
import com.milkrun.model.VanState;
import com.milkrun.model.VanState.DataConfidence;
import com.milkrun.model.VanState.EtaMethod;
import com.milkrun.model.VanState.SlaRisk;
import com.milkrun.model.VanStatus;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Mono;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Runs the analytics queries through Calcite against a real PostGIS database
 * (schema from the Flyway migrations) plus an in-memory live table.
 */
@Testcontainers(disabledWithoutDocker = true)
class AnalyticsFederationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(
            DockerImageName.parse("postgis/postgis:16-3.4").asCompatibleSubstituteFor("postgres"));

    static HikariDataSource dataSource;
    static CalciteSchemaFactory calcite;
    static AnalyticsService analytics;
    static final List<VanState> live = new CopyOnWriteArrayList<>();

    @BeforeAll
    static void setUp() throws Exception {
        dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(POSTGRES.getJdbcUrl());
        dataSource.setUsername(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());

        Flyway.configure().dataSource(dataSource).baselineOnMigrate(true).baselineVersion("0").load().migrate();

        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            // Two vans' completed routes: van-a 9/10 delivered, van-b 3/4
            st.execute("""
                    INSERT INTO completed_routes (route_id, van_id, planned_start, planned_end, total_stops,
                        completed_stops, failed_stops, status, avg_speed_kmh, total_distance_km)
                    VALUES ('r-a1', 'van-a', now(), now(), 10, 9, 1, 'COMPLETED', 24.0, 30.0),
                           ('r-b1', 'van-b', now(), now(), 4, 3, 1, 'COMPLETED', 20.0, 12.0),
                           ('r-c1', 'van-c', now(), now(), 5, 0, 0, 'IN_PROGRESS', NULL, NULL)
                    """);
            // Breaches: 3 in De Pijp (60, 120, 180 s), 1 at Central Station (30 s), 1 outside zones
            st.execute("""
                    INSERT INTO sla_breaches (route_id, van_id, stop_index, customer_id, sla_deadline,
                        predicted_arrival, actual_arrival, breach_seconds, severity, cause, geofence_id)
                    SELECT 'r-a1', v.van, 0, 'c', now(), NULL, now(), v.secs, v.sev, 'LATE_ARRIVAL',
                           (SELECT id FROM geofence_zones WHERE name = v.zone)
                    FROM (VALUES ('van-a', 60, 'WARNING', 'De Pijp School Zone'),
                                 ('van-a', 120, 'WARNING', 'De Pijp School Zone'),
                                 ('van-b', 180, 'CRITICAL', 'De Pijp School Zone'),
                                 ('van-b', 30, 'WARNING', 'Central Station Traffic'),
                                 ('van-c', 500, 'CRITICAL', NULL)) AS v(van, secs, sev, zone)
                    """);
            st.execute("""
                    INSERT INTO dead_letter_log (original_topic, van_id, event_payload, error_reason, reconciled)
                    VALUES ('gps-events', 'van-a', '{}', 'LATE_ARRIVAL', true),
                           ('gps-events', 'van-a', '{}', 'LATE_ARRIVAL', false),
                           ('gps-events', 'van-b', '{}', 'MALFORMED', false)
                    """);
        }

        calcite = new CalciteSchemaFactory(dataSource, new LiveVanTable(() -> live));
        analytics = new AnalyticsService(calcite, Duration.ZERO, Duration.ofSeconds(60));
    }

    @AfterAll
    static void tearDown() {
        calcite.shutdown();
        dataSource.close();
    }

    private static VanState van(String id, SlaRisk risk, Long slack, String zone) {
        return new VanState(id, "r-" + id, new Location(52.35, 4.89), 25, 0, 80, VanStatus.EN_ROUTE, 1, 10,
                60, risk, DataConfidence.REAL_TIME, zone != null, zone, Instant.now(),
                Instant.now(), Instant.now(), slack, EtaMethod.ROUTE);
    }

    private static List<Map<String, Object>> run(Mono<List<Map<String, Object>>> query) {
        return query.block(Duration.ofSeconds(60));
    }

    private static double num(Object o) {
        return ((Number) o).doubleValue();
    }

    @Test
    void delayZonesRankZonesByTotalBreachTime() {
        List<Map<String, Object>> rows = run(analytics.getTopDelayZones(10));
        assertEquals(2, rows.size(), rows.toString());
        Map<String, Object> top = rows.get(0);
        assertEquals("De Pijp School Zone", top.get("zone_name"));
        assertEquals(3, num(top.get("breach_count")));
        assertEquals(360, num(top.get("total_breach_seconds")));
        assertEquals(120.0, num(top.get("avg_breach_seconds")), 0.001);
        assertEquals("Central Station Traffic", rows.get(1).get("zone_name"));
    }

    @Test
    void slaSummaryGroupsBySeverity() {
        List<Map<String, Object>> rows = run(analytics.getSlaSummary());
        Map<String, Object> warning = rows.stream().filter(r -> "WARNING".equals(r.get("severity"))).findFirst().orElseThrow();
        assertEquals(3, num(warning.get("breach_count")));
        assertEquals(70.0, num(warning.get("avg_breach_seconds")), 0.001);
        assertEquals(120, num(warning.get("max_breach_seconds")));
        assertEquals("LATE_ARRIVAL", warning.get("cause"));
    }

    @Test
    void vanPerformanceComputesSuccessRatesOfCompletedRoutes() {
        List<Map<String, Object>> rows = run(analytics.getVanPerformance(20));
        assertEquals(2, rows.size(), "in-progress routes are excluded");
        assertEquals("van-a", rows.get(0).get("van_id"));
        assertEquals(90.0, num(rows.get(0).get("success_rate_pct")), 0.001);
        assertEquals(75.0, num(rows.get(1).get("success_rate_pct")), 0.001);
        assertEquals(24.0, num(rows.get(0).get("avg_speed")), 0.001);
    }

    @Test
    void dlqSummaryCountsReconciledAndPending() {
        List<Map<String, Object>> rows = run(analytics.getDlqSummary());
        Map<String, Object> late = rows.stream().filter(r -> "LATE_ARRIVAL".equals(r.get("error_reason"))).findFirst().orElseThrow();
        assertEquals(2, num(late.get("event_count")));
        assertEquals(1, num(late.get("reconciled_count")));
        assertEquals(1, num(late.get("pending_count")));
    }

    @Test
    void heatmapListsEveryZoneIncludingOnesWithoutBreaches() {
        List<Map<String, Object>> rows = run(analytics.getGeofenceHeatmapData());
        assertEquals(4, rows.size());
        Map<String, Object> vondelpark = rows.stream()
                .filter(r -> "Vondelpark Construction".equals(r.get("zone_name"))).findFirst().orElseThrow();
        assertEquals(0, num(vondelpark.get("hit_count")));
        assertEquals(0.0, num(vondelpark.get("avg_delay_seconds")), 0.001);
    }

    @Test
    void liveZoneRiskJoinsCurrentVansWithZoneHistory() {
        live.clear();
        live.add(van("van-1", SlaRisk.WARNING, 40L, "De Pijp School Zone"));
        live.add(van("van-2", SlaRisk.NONE, 300L, "Vondelpark Construction"));
        live.add(van("van-3", SlaRisk.CRITICAL, -20L, null));
        List<Map<String, Object>> rows = run(analytics.getLiveZoneRisk());

        assertEquals(2, rows.size(), "only vans inside a zone");
        assertEquals("van-1", rows.get(0).get("van_id"));
        assertEquals(3, num(rows.get(0).get("zone_breaches")));
        assertEquals(120.0, num(rows.get(0).get("zone_avg_breach_seconds")), 0.001);
        assertEquals(0.6, num(rows.get(0).get("speed_factor")), 0.001);
        assertEquals("van-2", rows.get(1).get("van_id"));
        assertEquals(0, num(rows.get(1).get("zone_breaches")));
    }

    @Test
    void atRiskVansIncludesEachVansBreachHistory() {
        live.clear();
        live.add(van("van-a", SlaRisk.CRITICAL, -30L, null));
        live.add(van("van-b", SlaRisk.WARNING, 20L, null));
        live.add(van("van-z", SlaRisk.NONE, 400L, null));
        List<Map<String, Object>> rows = run(analytics.getAtRiskVans());

        assertEquals(List.of("van-a", "van-b"), rows.stream().map(r -> r.get("van_id")).toList());
        assertEquals(2, num(rows.get(0).get("past_breaches")));
        assertEquals(90.0, num(rows.get(0).get("past_avg_breach_seconds")), 0.001);
        assertEquals(2, num(rows.get(1).get("past_breaches")));
    }

    @Test
    void aggregationIsPushedDownToPostgres() throws Exception {
        String plan = calcite.explain("""
                SELECT s.van_id, COUNT(*) AS n FROM milkrun.sla_breaches s GROUP BY s.van_id""");
        assertTrue(plan.contains("JdbcAggregate"), plan);
    }

    @Test
    void joinOfBreachesAndZonesIsPushedDownToPostgres() throws Exception {
        String plan = calcite.explain("""
                SELECT g.name, COUNT(*) AS n
                FROM milkrun.sla_breaches s JOIN milkrun.geofence_zones g ON s.geofence_id = g.id
                GROUP BY g.name""");
        assertTrue(plan.contains("JdbcJoin") && plan.contains("JdbcAggregate"), plan);
    }

    @Test
    void queriesRunOffTheCallersThread() {
        AtomicReference<String> thread = new AtomicReference<>();
        analytics.getSlaSummary().doOnNext(r -> thread.set(Thread.currentThread().getName())).block();
        assertTrue(thread.get().startsWith("calcite"), thread.get());
    }

    @Test
    void delayZonesStaysFastOnALargeBreachTable() throws Exception {
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            st.execute("""
                    INSERT INTO sla_breaches (route_id, van_id, stop_index, customer_id, sla_deadline,
                        actual_arrival, breach_seconds, severity, cause, geofence_id)
                    SELECT 'bulk', 'van-' || (i % 50), 0, 'c', now(), now(), i % 600, 'WARNING', 'LATE_ARRIVAL',
                           (SELECT id FROM geofence_zones ORDER BY name OFFSET (i % 5) LIMIT 1)
                    FROM generate_series(1, 200000) AS i
                    """);
            st.execute("ANALYZE sla_breaches");
        }
        run(analytics.getTopDelayZones(5)); // warm up
        long start = System.nanoTime();
        List<Map<String, Object>> rows = run(analytics.getTopDelayZones(5));
        long ms = (System.nanoTime() - start) / 1_000_000;
        assertEquals(4, rows.size());
        assertTrue(ms < 2000, "delay-zones took " + ms + " ms");
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            st.execute("DELETE FROM sla_breaches WHERE route_id = 'bulk'");
        }
    }
}
