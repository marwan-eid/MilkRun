package com.milkrun.engine;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RouteGeometryTest {

    /** Straight line due north along lon 4.85, a vertex every 0.0005° (~55.6 m). */
    static double[][] northLine(int vertices) {
        double[][] w = new double[vertices][];
        for (int i = 0; i < vertices; i++) {
            w[i] = new double[] { 52.300 + i * 0.0005, 4.85 };
        }
        return w;
    }

    private static final double VERTEX_SPACING_M = RouteGeometry.haversineMeters(52.300, 4.85, 52.3005, 4.85);

    @Test
    void cumulativeDistances() {
        RouteGeometry g = RouteGeometry.of(northLine(41), GeofenceDetector.withZones(List.of()));
        assertEquals(41, g.vertexCount());
        assertEquals(40 * VERTEX_SPACING_M, g.length(), 0.01);
        assertEquals(10 * VERTEX_SPACING_M, g.distanceAt(10), 0.01);
    }

    @Test
    void locateProjectsOntoTheLeg() {
        RouteGeometry g = RouteGeometry.of(northLine(41), GeofenceDetector.withZones(List.of()));
        // 30 m east of the point halfway between vertices 10 and 11
        double lonOffset = 30 / (111_195 * Math.cos(Math.toRadians(52.30525)));
        RouteGeometry.Match m = g.locate(52.30525, 4.85 + lonOffset, 0, 20);
        assertEquals(10, m.segment());
        assertEquals(10.5 * VERTEX_SPACING_M, m.distanceAlong(), 0.5);
        assertEquals(30, m.offRouteMeters(), 0.5);
    }

    @Test
    void locateOnlySearchesTheGivenLeg() {
        RouteGeometry g = RouteGeometry.of(northLine(41), GeofenceDetector.withZones(List.of()));
        // The point sits at vertex 30, but the leg is 0..20, so the closest leg point is vertex 20
        RouteGeometry.Match m = g.locate(52.315, 4.85, 0, 20);
        assertEquals(20 * VERTEX_SPACING_M, m.distanceAlong(), 0.01);
        assertEquals(10 * VERTEX_SPACING_M, m.offRouteMeters(), 0.5);
    }

    @Test
    void travelTimeUsesZoneFactors() {
        // Zone slows segments whose midpoints lie between vertex 10 and 20 to half speed
        GeofenceDetector zones = GeofenceDetector.withZones(List.of(
                GeofenceDetector.Zone.rectangle("z", "Half", 0.5, 4.84, 52.305, 4.86, 52.310)));
        RouteGeometry g = RouteGeometry.of(northLine(41), zones);
        double v = 10; // m/s
        // 10 free segments + 10 half-speed segments
        double expected = 10 * VERTEX_SPACING_M / v + 10 * VERTEX_SPACING_M / (v * 0.5);
        assertEquals(expected, g.travelSeconds(0, 20, v), 0.01);
        assertEquals(0.5, g.factorAt(12 * VERTEX_SPACING_M));
        assertEquals(1.0, g.factorAt(25 * VERTEX_SPACING_M));
    }

    @Test
    void travelTimeFromTheMiddleOfASegment() {
        RouteGeometry g = RouteGeometry.of(northLine(41), GeofenceDetector.withZones(List.of()));
        double from = 2.5 * VERTEX_SPACING_M;
        assertEquals((10 - 2.5) * VERTEX_SPACING_M / 10, g.travelSeconds(from, 10, 10), 0.01);
        assertEquals(0, g.travelSeconds(g.distanceAt(12), 10, 10), "already past the target");
    }

    @Test
    void recordsTheZonesVersionItWasBuiltWith() {
        GeofenceDetector zones = GeofenceDetector.withZones(List.of());
        RouteGeometry g = RouteGeometry.of(northLine(3), zones);
        assertEquals(zones.version(), g.zonesVersion());
    }
}
