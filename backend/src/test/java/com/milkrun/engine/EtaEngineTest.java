package com.milkrun.engine;

import com.milkrun.model.Location;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class EtaEngineTest {

    @Test
    void haversineDistanceSamePointShouldBeZero() {
        Location p = new Location(52.37, 4.90);
        assertEquals(0.0, EtaEngine.haversineDistance(p, p), 0.001);
    }

    @Test
    void haversineDistanceAmsterdamCentralToDamSquare() {
        Location centraal = new Location(52.3791, 4.9003);
        Location dam = new Location(52.3730, 4.8932);
        double dist = EtaEngine.haversineDistance(centraal, dam);

        // Expected ~0.8 km
        assertTrue(dist > 0.5, "Distance should be > 0.5 km, got: " + dist);
        assertTrue(dist < 1.5, "Distance should be < 1.5 km, got: " + dist);
    }

    @Test
    void haversineDistanceShouldBeSymmetric() {
        Location a = new Location(52.37, 4.90);
        Location b = new Location(52.38, 4.92);
        assertEquals(
                EtaEngine.haversineDistance(a, b),
                EtaEngine.haversineDistance(b, a),
                0.001
        );
    }

    @Test
    void haversineDistanceLongerDistances() {
        // Amsterdam to Rotterdam ≈ 57 km
        Location amsterdam = new Location(52.3676, 4.9041);
        Location rotterdam = new Location(51.9244, 4.4777);
        double dist = EtaEngine.haversineDistance(amsterdam, rotterdam);

        assertTrue(dist > 50, "Amsterdam-Rotterdam should be > 50 km, got: " + dist);
        assertTrue(dist < 65, "Amsterdam-Rotterdam should be < 65 km, got: " + dist);
    }
}
