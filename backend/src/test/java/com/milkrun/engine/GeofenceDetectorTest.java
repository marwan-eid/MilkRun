package com.milkrun.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.milkrun.model.Location;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class GeofenceDetectorTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private GeofenceDetector.Zone zone(String geojson) throws Exception {
        return GeofenceDetector.Zone.fromGeoJson("7d9c2f36-0000-0000-0000-000000000001", "Zone", "TRAFFIC", 0.5,
                mapper.readTree(geojson));
    }

    @Test
    void detectsPointsInsideARectangleOnly() throws Exception {
        // De Pijp School Zone from the seed data
        GeofenceDetector.Zone z = zone("""
                {"type":"Polygon","coordinates":[[[4.89,52.35],[4.898,52.35],[4.898,52.354],[4.89,52.354],[4.89,52.35]]]}""");
        assertTrue(z.contains(52.352, 4.894));
        assertFalse(z.contains(52.3545, 4.894), "just north of the zone");
        assertFalse(z.contains(52.352, 4.8985), "just east of the zone");
        // The old detector widened every box by 0.01°; this point is ~500 m outside
        assertFalse(z.contains(52.3585, 4.894));
    }

    @Test
    void handlesNonRectangularPolygons() throws Exception {
        // Triangle with its right angle at the south-west corner
        GeofenceDetector.Zone z = zone("""
                {"type":"Polygon","coordinates":[[[4.0,52.0],[4.1,52.0],[4.0,52.1],[4.0,52.0]]]}""");
        assertTrue(z.contains(52.02, 4.02));
        assertFalse(z.contains(52.08, 4.08), "inside the bounding box but outside the triangle");
    }

    @Test
    void excludesHoles() throws Exception {
        GeofenceDetector.Zone z = zone("""
                {"type":"Polygon","coordinates":[
                  [[4.0,52.0],[4.1,52.0],[4.1,52.1],[4.0,52.1],[4.0,52.0]],
                  [[4.04,52.04],[4.06,52.04],[4.06,52.06],[4.04,52.06],[4.04,52.04]]]}""");
        assertTrue(z.contains(52.02, 4.02));
        assertFalse(z.contains(52.05, 4.05), "inside the hole");
    }

    @Test
    void supportsMultiPolygons() throws Exception {
        GeofenceDetector.Zone z = zone("""
                {"type":"MultiPolygon","coordinates":[
                  [[[4.0,52.0],[4.01,52.0],[4.01,52.01],[4.0,52.01],[4.0,52.0]]],
                  [[[4.1,52.1],[4.11,52.1],[4.11,52.11],[4.1,52.11],[4.1,52.1]]]]}""");
        assertTrue(z.contains(52.005, 4.005));
        assertTrue(z.contains(52.105, 4.105));
        assertFalse(z.contains(52.05, 4.05));
    }

    @Test
    void checkReportsZoneAndFactor() {
        GeofenceDetector detector = GeofenceDetector.withZones(List.of(
                GeofenceDetector.Zone.rectangle("id-1", "Slow street", 0.4, 4.0, 52.0, 4.1, 52.1)));
        GeofenceDetector.GeofenceResult inside = detector.check(new Location(52.05, 4.05));
        assertTrue(inside.inGeofence());
        assertEquals("Slow street", inside.zoneName());
        assertEquals("id-1", inside.zoneId());
        assertEquals(0.4, detector.speedFactorAt(52.05, 4.05));

        assertFalse(detector.check(new Location(51.9, 4.05)).inGeofence());
        assertEquals(1.0, detector.speedFactorAt(51.9, 4.05));
    }

    @Test
    void versionChangesOnlyWhenZonesChange() {
        GeofenceDetector.Zone a = GeofenceDetector.Zone.rectangle("id-1", "A", 0.5, 4.0, 52.0, 4.1, 52.1);
        GeofenceDetector detector = GeofenceDetector.withZones(List.of(a));
        int v1 = detector.version();
        detector.replaceZones(List.of(GeofenceDetector.Zone.rectangle("id-1", "A", 0.5, 4.0, 52.0, 4.1, 52.1)));
        assertEquals(v1, detector.version(), "same zones reloaded");
        detector.replaceZones(List.of(GeofenceDetector.Zone.rectangle("id-1", "A", 0.3, 4.0, 52.0, 4.1, 52.1)));
        assertEquals(v1 + 1, detector.version(), "speed factor changed");
    }
}
