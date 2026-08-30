package com.milkrun.calcite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * Analytics service powered by Apache Calcite.
 *
 * Provides pre-built analytical queries that run through Calcite's
 * query federation layer. These queries demonstrate Calcite's role as
 * a SQL parser/optimizer sitting atop PostgreSQL.
 *
 * In production, these could federate across PostgreSQL + Elasticsearch
 * or combine real-time in-memory state with historical DB data.
 */
@Service
public class AnalyticsService {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsService.class);

    private final CalciteSchemaFactory calciteSchema;

    public AnalyticsService(CalciteSchemaFactory calciteSchema) {
        this.calciteSchema = calciteSchema;
    }

    /**
     * Top delay zones by total SLA breach seconds.
     * Joins geofence_zones → sla_breaches to find which zones cause the most
     * delays.
     */
    public List<Map<String, Object>> getTopDelayZones(int limit) {
        String sql = String.format("""
                SELECT
                    g.name AS zone_name,
                    g.zone_type AS zone_type,
                    g.speed_factor AS speed_factor,
                    COUNT(s.id) AS breach_count,
                    COALESCE(SUM(s.breach_seconds), 0) AS total_breach_seconds,
                    AVG(s.breach_seconds) AS avg_breach_seconds
                FROM geofence_zones g
                JOIN sla_breaches s ON g.id = s.geofence_id
                WHERE g.active = true
                GROUP BY g.name, g.zone_type, g.speed_factor
                ORDER BY total_breach_seconds DESC
                FETCH FIRST %d ROWS ONLY
                """, limit);

        return calciteSchema.executeQuery(sql);
    }

    /**
     * SLA breach summary — aggregated breach stats.
     */
    public List<Map<String, Object>> getSlaSummary() {
        String sql = """
                SELECT
                    "severity",
                    COUNT(*) AS breach_count,
                    COALESCE(AVG("breach_seconds"), 0) AS avg_breach_seconds,
                    COALESCE(MAX("breach_seconds"), 0) AS max_breach_seconds,
                    "cause"
                FROM "sla_breaches"
                GROUP BY "severity", "cause"
                ORDER BY breach_count DESC
                """;

        return calciteSchema.executeQuery(sql);
    }

    /**
     * Van performance rankings — completed routes, success rate, avg speed.
     */
    public List<Map<String, Object>> getVanPerformance(int limit) {
        String sql = String.format("""
                SELECT
                    r.van_id,
                    COUNT(r.id) AS total_routes,
                    SUM(r.completed_stops) AS total_completed,
                    SUM(r.failed_stops) AS total_failed,
                    CASE
                        WHEN SUM(r.completed_stops) + SUM(r.failed_stops) > 0
                        THEN (SUM(r.completed_stops) * 100.0) /
                             (SUM(r.completed_stops) + SUM(r.failed_stops))
                        ELSE 0.0
                    END AS success_rate_pct,
                    COALESCE(AVG(r.avg_speed_kmh), 0) AS avg_speed,
                    COALESCE(AVG(r.total_distance_km), 0) AS avg_distance_km
                FROM completed_routes r
                WHERE r.status = 'COMPLETED'
                GROUP BY r.van_id
                ORDER BY success_rate_pct DESC, total_routes DESC
                FETCH FIRST %d ROWS ONLY
                """, limit);

        return calciteSchema.executeQuery(sql);
    }

    /**
     * Dead letter queue summary — errors by reason.
     */
    public List<Map<String, Object>> getDlqSummary() {
        String sql = """
                SELECT
                    "error_reason",
                    COUNT(*) AS event_count,
                    SUM(CASE WHEN "reconciled" = true THEN 1 ELSE 0 END) AS reconciled_count,
                    SUM(CASE WHEN "reconciled" = false THEN 1 ELSE 0 END) AS pending_count
                FROM "dead_letter_log"
                GROUP BY "error_reason"
                ORDER BY event_count DESC
                """;

        return calciteSchema.executeQuery(sql);
    }

    /**
     * Geofence zone data for heatmap overlay (returns zone geometry bounds + hit
     * count).
     */
    public List<Map<String, Object>> getGeofenceHeatmapData() {
        String sql = """
                SELECT
                    g."name" AS zone_name,
                    g."zone_type",
                    g."speed_factor",
                    COUNT(s."id") AS hit_count,
                    COALESCE(AVG(s."breach_seconds"), 0) AS avg_delay_seconds
                FROM "geofence_zones" g
                LEFT JOIN "sla_breaches" s ON g."id" = s."geofence_id"
                WHERE g."active" = true
                GROUP BY g."name", g."zone_type", g."speed_factor"
                """;

        return calciteSchema.executeQuery(sql);
    }

    public boolean isReady() {
        return calciteSchema.isReady();
    }
}
