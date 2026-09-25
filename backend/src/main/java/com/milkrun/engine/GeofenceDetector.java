package com.milkrun.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.milkrun.model.Location;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Point-in-polygon lookups against the delay zones stored in PostGIS.
 *
 * Zone polygons are read with ST_AsGeoJSON and tested in memory with ray
 * casting (after a bounding-box check), which avoids a database round trip for
 * every GPS event. The zones are reloaded periodically, so edits to
 * geofence_zones take effect without a restart and a database that was not
 * ready at startup is picked up later.
 */
@Service
public class GeofenceDetector {

    private static final Logger log = LoggerFactory.getLogger(GeofenceDetector.class);

    private final DatabaseClient databaseClient;
    private final ObjectMapper objectMapper;
    private final Duration refreshInterval;
    private volatile Snapshot snapshot = new Snapshot(List.of(), 0);
    private Disposable refresher;

    @Autowired
    public GeofenceDetector(DatabaseClient databaseClient, ObjectMapper objectMapper,
            @Value("${milkrun.geofence.refresh-interval:60s}") Duration refreshInterval) {
        this.databaseClient = databaseClient;
        this.objectMapper = objectMapper;
        this.refreshInterval = refreshInterval;
    }

    /** For tests: a detector with fixed zones and no database. */
    public static GeofenceDetector withZones(List<Zone> zones) {
        GeofenceDetector detector = new GeofenceDetector(null, null, Duration.ZERO);
        detector.replaceZones(zones);
        return detector;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        refresher = Flux.interval(Duration.ZERO, refreshInterval)
                .onBackpressureDrop()
                .concatMap(tick -> load()
                        .doOnError(e -> log.warn("Loading geofence zones failed: {}", e.getMessage()))
                        .onErrorResume(e -> Mono.empty()))
                .subscribe();
    }

    @PreDestroy
    void stop() {
        if (refresher != null) {
            refresher.dispose();
        }
    }

    Mono<Void> load() {
        return databaseClient.sql("""
                SELECT id, name, zone_type, speed_factor, ST_AsGeoJSON(geometry) AS geojson
                FROM geofence_zones
                WHERE active = true
                ORDER BY name
                """)
                .fetch()
                .all()
                .map(this::toZone)
                .collectList()
                .doOnNext(this::replaceZones)
                .then();
    }

    private Zone toZone(Map<String, Object> row) {
        try {
            return Zone.fromGeoJson(
                    row.get("id").toString(),
                    (String) row.get("name"),
                    (String) row.get("zone_type"),
                    ((Number) row.get("speed_factor")).doubleValue(),
                    objectMapper.readTree((String) row.get("geojson")));
        } catch (Exception e) {
            throw new IllegalStateException("Bad geometry for zone " + row.get("name"), e);
        }
    }

    /** Installs a new set of zones; the version only changes if the zones did. */
    public synchronized void replaceZones(List<Zone> zones) {
        Snapshot current = snapshot;
        if (!sameZones(current.zones(), zones)) {
            snapshot = new Snapshot(List.copyOf(zones), current.version() + 1);
            log.info("Loaded {} geofence zones (version {})", zones.size(), current.version() + 1);
        }
    }

    private static boolean sameZones(List<Zone> a, List<Zone> b) {
        if (a.size() != b.size()) return false;
        for (int i = 0; i < a.size(); i++) {
            if (!a.get(i).signature().equals(b.get(i).signature())) return false;
        }
        return true;
    }

    /** Increments whenever the loaded zones change. */
    public int version() {
        return snapshot.version();
    }

    public List<Zone> zones() {
        return snapshot.zones();
    }

    public GeofenceResult check(Location location) {
        Zone zone = zoneAt(location.latitude(), location.longitude());
        return zone == null
                ? GeofenceResult.OUTSIDE
                : new GeofenceResult(true, zone.id(), zone.name(), zone.zoneType(), zone.speedFactor());
    }

    /** Speed multiplier at a point: the zone's factor, or 1.0 outside all zones. */
    public double speedFactorAt(double lat, double lon) {
        Zone zone = zoneAt(lat, lon);
        return zone == null ? 1.0 : zone.speedFactor();
    }

    private Zone zoneAt(double lat, double lon) {
        for (Zone zone : snapshot.zones()) {
            if (zone.contains(lat, lon)) {
                return zone;
            }
        }
        return null;
    }

    public record GeofenceResult(boolean inGeofence, String zoneId, String zoneName, String zoneType,
            double speedFactor) {
        public static final GeofenceResult OUTSIDE = new GeofenceResult(false, null, null, null, 1.0);
    }

    private record Snapshot(List<Zone> zones, int version) {
    }

    /**
     * A zone made of one or more polygons, each an outer ring plus optional
     * holes. Rings are arrays of [longitude, latitude] (GeoJSON order).
     */
    public record Zone(String id, String name, String zoneType, double speedFactor,
            List<List<double[][]>> polygons, double minLat, double maxLat, double minLon, double maxLon,
            String signature) {

        public static Zone fromGeoJson(String id, String name, String zoneType, double speedFactor, JsonNode geo) {
            List<List<double[][]>> polygons = new ArrayList<>();
            String type = geo.path("type").asText();
            JsonNode coords = geo.path("coordinates");
            if ("Polygon".equals(type)) {
                polygons.add(rings(coords));
            } else if ("MultiPolygon".equals(type)) {
                coords.forEach(p -> polygons.add(rings(p)));
            } else {
                throw new IllegalArgumentException("Unsupported geometry type " + type);
            }
            return of(id, name, zoneType, speedFactor, polygons, geo.toString());
        }

        /** Rectangle zone, handy for tests. */
        public static Zone rectangle(String id, String name, double speedFactor,
                double minLon, double minLat, double maxLon, double maxLat) {
            double[][] ring = { { minLon, minLat }, { maxLon, minLat }, { maxLon, maxLat }, { minLon, maxLat }, { minLon, minLat } };
            return of(id, name, "TEST", speedFactor, List.of(List.<double[][]>of(ring)),
                    name + minLon + minLat + maxLon + maxLat);
        }

        private static Zone of(String id, String name, String zoneType, double speedFactor,
                List<List<double[][]>> polygons, String geometryText) {
            double minLat = Double.POSITIVE_INFINITY, maxLat = Double.NEGATIVE_INFINITY;
            double minLon = Double.POSITIVE_INFINITY, maxLon = Double.NEGATIVE_INFINITY;
            for (List<double[][]> polygon : polygons) {
                for (double[] p : polygon.get(0)) {
                    minLon = Math.min(minLon, p[0]);
                    maxLon = Math.max(maxLon, p[0]);
                    minLat = Math.min(minLat, p[1]);
                    maxLat = Math.max(maxLat, p[1]);
                }
            }
            String signature = String.join("|", Objects.toString(id), name, Double.toString(speedFactor), geometryText);
            return new Zone(id, name, zoneType, speedFactor, polygons, minLat, maxLat, minLon, maxLon, signature);
        }

        private static List<double[][]> rings(JsonNode polygon) {
            List<double[][]> rings = new ArrayList<>();
            polygon.forEach(ring -> {
                double[][] points = new double[ring.size()][];
                for (int i = 0; i < ring.size(); i++) {
                    points[i] = new double[] { ring.get(i).get(0).asDouble(), ring.get(i).get(1).asDouble() };
                }
                rings.add(points);
            });
            if (rings.isEmpty()) {
                throw new IllegalArgumentException("Polygon without rings");
            }
            return rings;
        }

        public boolean contains(double lat, double lon) {
            if (lat < minLat || lat > maxLat || lon < minLon || lon > maxLon) {
                return false;
            }
            for (List<double[][]> polygon : polygons) {
                if (inRing(polygon.get(0), lat, lon)) {
                    boolean inHole = false;
                    for (int h = 1; h < polygon.size() && !inHole; h++) {
                        inHole = inRing(polygon.get(h), lat, lon);
                    }
                    if (!inHole) return true;
                }
            }
            return false;
        }

        /** Ray casting: count crossings of a ray from the point towards +longitude. */
        static boolean inRing(double[][] ring, double lat, double lon) {
            boolean inside = false;
            for (int i = 0, j = ring.length - 1; i < ring.length; j = i++) {
                double xi = ring[i][0], yi = ring[i][1];
                double xj = ring[j][0], yj = ring[j][1];
                if ((yi > lat) != (yj > lat) && lon < (xj - xi) * (lat - yi) / (yj - yi) + xi) {
                    inside = !inside;
                }
            }
            return inside;
        }
    }
}
