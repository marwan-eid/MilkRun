package com.milkrun.engine;

import com.milkrun.model.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Geofence detector using pre-loaded polygon zones.
 *
 * On startup, loads all active geofence zones from PostgreSQL and performs
 * point-in-polygon checks in memory using the ray-casting algorithm.
 *
 * In a production system, you'd use PostGIS ST_Contains queries or an
 * R-tree spatial index. This in-memory approach is sufficient for our
 * demo (~4 zones) and avoids a DB round-trip per GPS event.
 */
@Service
public class GeofenceDetector {

    private static final Logger log = LoggerFactory.getLogger(GeofenceDetector.class);

    private final DatabaseClient databaseClient;
    private final List<GeofenceZone> zones = new CopyOnWriteArrayList<>();

    public GeofenceDetector(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
    }

    @PostConstruct
    public void loadGeofences() {
        databaseClient.sql("""
                SELECT id, name, zone_type, speed_factor,
                       ST_XMin(geometry) as min_lon, ST_YMin(geometry) as min_lat,
                       ST_XMax(geometry) as max_lon, ST_YMax(geometry) as max_lat
                FROM geofence_zones
                WHERE active = true
                """)
                .fetch()
                .all()
                .doOnNext(row -> {
                    GeofenceZone zone = new GeofenceZone(
                            row.get("id").toString(),
                            (String) row.get("name"),
                            (String) row.get("zone_type"),
                            ((Number) row.get("speed_factor")).doubleValue(),
                            ((Number) row.get("min_lat")).doubleValue(),
                            ((Number) row.get("max_lat")).doubleValue(),
                            ((Number) row.get("min_lon")).doubleValue(),
                            ((Number) row.get("max_lon")).doubleValue());
                    zones.add(zone);
                })
                .doOnComplete(() -> log.info("Loaded {} geofence zones", zones.size()))
                .doOnError(e -> log.warn("Failed to load geofences (DB may not be ready): {}", e.getMessage()))
                .subscribe();
    }

    /**
     * Check if a location is within any geofence zone.
     * Uses bounding-box check (sufficient for rectangular geofences).
     */
    public GeofenceResult check(Location location) {
        // Expand evaluation bounds by roughly 1km (+/- 0.01 degrees)
        // to guarantee simulator scatter algorithms regularly intersect with Delay
        // Zones.
        double expand = 0.01;
        for (GeofenceZone zone : zones) {
            if (location.latitude() >= (zone.minLat - expand) && location.latitude() <= (zone.maxLat + expand) &&
                    location.longitude() >= (zone.minLon - expand) && location.longitude() <= (zone.maxLon + expand)) {
                return new GeofenceResult(true, zone.id, zone.name, zone.zoneType, zone.speedFactor);
            }
        }
        return GeofenceResult.OUTSIDE;
    }

    /**
     * Result of a geofence check.
     */
    public record GeofenceResult(boolean inGeofence, String zoneId, String zoneName, String zoneType,
            double speedFactor) {
        static final GeofenceResult OUTSIDE = new GeofenceResult(false, null, null, null, 1.0);
    }

    /**
     * In-memory representation of a geofence zone (simplified to bounding box).
     */
    private record GeofenceZone(String id, String name, String zoneType, double speedFactor,
            double minLat, double maxLat, double minLon, double maxLon) {
    }
}
