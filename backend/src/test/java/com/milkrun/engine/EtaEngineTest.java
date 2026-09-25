package com.milkrun.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.milkrun.model.GpsEvent;
import com.milkrun.model.Location;
import com.milkrun.model.RoutePlan;
import com.milkrun.model.VanState;
import com.milkrun.model.VanState.EtaMethod;
import com.milkrun.model.VanState.SlaRisk;
import com.milkrun.model.VanStatus;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class EtaEngineTest {

    private static final String VAN = "van-001";
    private static final String ROUTE = "route-1";
    private static final Instant T0 = Instant.parse("2026-09-25T10:00:00Z");
    /** 0.0005° of latitude between route vertices. */
    private static final double SPACING_M = RouteGeometry.haversineMeters(52.300, 4.85, 52.3005, 4.85);
    /** Base 36 km/h simulated x time scale 4 = 40 m/s wall-clock. */
    private static final double PRIOR_MPS = 40;

    private SimpleMeterRegistry registry;
    private GeofenceDetector geofence;
    private RoutePlanStore store;
    private CircuitBreaker breaker;
    private EtaEngine engine;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        geofence = GeofenceDetector.withZones(List.of());
        store = new RoutePlanStore(new ObjectMapper().findAndRegisterModules(), geofence, null, registry, "unused", "route-plans");
        breaker = CircuitBreaker.ofDefaults("eta");
        engine = new EtaEngine(breaker, geofence, store, registry, 60, 0, 1.3, 200);
    }

    /** Route north along lon 4.85 with stops at vertices 20 and 30 and the hub at vertex 40. */
    private void plan(Instant deadline0, Instant deadline1) {
        store.put(new RoutePlan(ROUTE, VAN, 0, T0, 4, 36,
                List.of(stop(0, "c0", 20, deadline0), stop(1, "c1", 30, deadline1)),
                RouteGeometryTest.northLine(41)));
    }

    private static RoutePlan.Stop stop(int index, String customer, int vertex, Instant deadline) {
        return new RoutePlan.Stop(index, customer, new Location(52.300 + vertex * 0.0005, 4.85),
                deadline, deadline.minusSeconds(60), 1, vertex);
    }

    private static GpsEvent gps(Instant at, int vertex, double speedKmh, int stopIndex, VanStatus status) {
        return gpsAt(at, 52.300 + vertex * 0.0005, 4.85, speedKmh, stopIndex, status);
    }

    private static GpsEvent gpsAt(Instant at, double lat, double lon, double speedKmh, int stopIndex, VanStatus status) {
        return new GpsEvent(UUID.randomUUID(), VAN, at.toEpochMilli(), at, at.plusMillis(300),
                new Location(lat, lon), speedKmh, 0, 90, ROUTE, stopIndex, 2, status);
    }

    @Test
    void unknownWithoutARoutePlan() {
        VanState s = engine.processGpsEvent(gps(T0, 5, 36, 0, VanStatus.EN_ROUTE));
        assertEquals(SlaRisk.UNKNOWN, s.slaRisk());
        assertEquals(-1, s.etaNextStopSeconds());
        assertEquals(EtaMethod.NONE, s.etaMethod());
        assertNull(s.slaSlackSeconds());
    }

    @Test
    void etaIsRemainingRouteDistanceOverPriorSpeed() {
        plan(T0.plusSeconds(600), T0.plusSeconds(900));
        VanState s = engine.processGpsEvent(gps(T0, 10, 36, 0, VanStatus.EN_ROUTE));
        long expected = Math.round(10 * SPACING_M / PRIOR_MPS);
        assertEquals(expected, s.etaNextStopSeconds());
        assertEquals(EtaMethod.ROUTE, s.etaMethod());
        assertEquals(T0.plusSeconds(expected), s.predictedArrival());
        assertEquals(600 - expected, s.slaSlackSeconds());
        assertEquals(SlaRisk.NONE, s.slaRisk());
    }

    @Test
    void riskFollowsSlackAgainstTheDeadline() {
        long eta = Math.round(10 * SPACING_M / PRIOR_MPS); // ~14 s
        plan(T0.plusSeconds(eta + 61), T0.plusSeconds(900));
        assertEquals(SlaRisk.NONE, engine.processGpsEvent(gps(T0, 10, 36, 0, VanStatus.EN_ROUTE)).slaRisk());

        setUp();
        plan(T0.plusSeconds(eta + 30), T0.plusSeconds(900));
        assertEquals(SlaRisk.WARNING, engine.processGpsEvent(gps(T0, 10, 36, 0, VanStatus.EN_ROUTE)).slaRisk());

        setUp();
        plan(T0.plusSeconds(eta - 5), T0.plusSeconds(900));
        VanState late = engine.processGpsEvent(gps(T0, 10, 36, 0, VanStatus.EN_ROUTE));
        assertEquals(SlaRisk.CRITICAL, late.slaRisk());
        assertEquals(-5, late.slaSlackSeconds());
    }

    @Test
    void learnsSpeedFromProgressAlongTheRoute() {
        plan(T0.plusSeconds(600), T0.plusSeconds(900));
        engine.processGpsEvent(gps(T0, 10, 36, 0, VanStatus.EN_ROUTE));
        // Two vertices (~111 m) in 5 s: 22.2 m/s, much slower than the 40 m/s prior
        VanState s = engine.processGpsEvent(gps(T0.plusSeconds(5), 12, 36, 0, VanStatus.EN_ROUTE));
        double measured = 2 * SPACING_M / 5;
        assertEquals(Math.round(8 * SPACING_M / measured), s.etaNextStopSeconds());
    }

    @Test
    void slowsTheEstimateThroughDelayZones() {
        // Half-speed zone over the segments between vertices 14 and 20
        geofence.replaceZones(List.of(GeofenceDetector.Zone.rectangle("z", "Works", 0.5, 4.84, 52.307, 4.86, 52.3105)));
        plan(T0.plusSeconds(600), T0.plusSeconds(900));
        VanState s = engine.processGpsEvent(gps(T0, 10, 36, 0, VanStatus.EN_ROUTE));
        double expected = 4 * SPACING_M / PRIOR_MPS + 6 * SPACING_M / (PRIOR_MPS * 0.5);
        assertEquals(Math.round(expected), s.etaNextStopSeconds());
    }

    @Test
    void whileDeliveringReportsArrivalAgainstTheDeadline() {
        plan(T0.plusSeconds(20), T0.plusSeconds(900));
        VanState onTime = engine.processGpsEvent(gps(T0.plusSeconds(15), 20, 0, 0, VanStatus.DELIVERING));
        assertEquals(0, onTime.etaNextStopSeconds());
        assertEquals(SlaRisk.NONE, onTime.slaRisk());
        assertEquals(5, onTime.slaSlackSeconds());

        // Still delivering after the slot ended: the arrival time, not "now", decides
        VanState later = engine.processGpsEvent(gps(T0.plusSeconds(40), 20, 0, 0, VanStatus.DELIVERING));
        assertEquals(SlaRisk.NONE, later.slaRisk());
        assertEquals(T0.plusSeconds(15), later.predictedArrival());
    }

    @Test
    void lateArrivalIsCritical() {
        plan(T0.plusSeconds(10), T0.plusSeconds(900));
        VanState s = engine.processGpsEvent(gps(T0.plusSeconds(30), 20, 0, 0, VanStatus.DELIVERING));
        assertEquals(SlaRisk.CRITICAL, s.slaRisk());
        assertEquals(-20, s.slaSlackSeconds());
    }

    @Test
    void recordsEtaErrorOnArrivalAndKeepsTheInitialPrediction() {
        plan(T0.plusSeconds(600), T0.plusSeconds(900));
        VanState first = engine.processGpsEvent(gps(T0, 10, 36, 0, VanStatus.EN_ROUTE));
        engine.processGpsEvent(gps(T0.plusSeconds(5), 12, 36, 0, VanStatus.EN_ROUTE));
        Instant arrival = first.predictedArrival().plusSeconds(7);
        engine.processGpsEvent(gps(arrival, 20, 0, 0, VanStatus.DELIVERING));

        assertEquals(first.predictedArrival(), engine.initialPrediction(VAN, ROUTE, "c0"));
        var summary = registry.find("milkrun.eta.error.seconds").summary();
        assertEquals(1, summary.count());
        assertEquals(7.0, summary.totalAmount(), 0.001);
    }

    @Test
    void offRouteFallsBackToStraightLine() {
        plan(T0.plusSeconds(600), T0.plusSeconds(900));
        // ~680 m east of the route
        VanState s = engine.processGpsEvent(gpsAt(T0, 52.305, 4.86, 36, 0, VanStatus.EN_ROUTE));
        assertEquals(EtaMethod.STRAIGHT_LINE, s.etaMethod());
        double straight = RouteGeometry.haversineMeters(52.305, 4.86, 52.310, 4.85) * 1.3;
        assertEquals(Math.round(straight / PRIOR_MPS), s.etaNextStopSeconds());
        assertEquals(1.0, registry.find("milkrun.eta.off_route").counter().count());
    }

    @Test
    void openBreakerFallsBackToStraightLine() {
        plan(T0.plusSeconds(600), T0.plusSeconds(900));
        breaker.transitionToForcedOpenState();
        VanState s = engine.processGpsEvent(gps(T0, 10, 36, 0, VanStatus.EN_ROUTE));
        assertEquals(EtaMethod.STRAIGHT_LINE, s.etaMethod());
        assertTrue(s.etaNextStopSeconds() > 0);
        assertEquals(1.0, registry.find("milkrun.circuit_breaker.fallbacks").counter().count());
    }

    @Test
    void noNextStopWhenReturning() {
        plan(T0.plusSeconds(600), T0.plusSeconds(900));
        VanState s = engine.processGpsEvent(gps(T0, 35, 36, 2, VanStatus.RETURNING));
        assertEquals(SlaRisk.NONE, s.slaRisk());
        assertEquals(-1, s.etaNextStopSeconds());
    }

    @Test
    void ignoresAPlanForAnotherRoute() {
        plan(T0.plusSeconds(600), T0.plusSeconds(900));
        GpsEvent other = new GpsEvent(UUID.randomUUID(), VAN, 1, T0, T0, new Location(52.305, 4.85),
                36, 0, 90, "route-2", 0, 2, VanStatus.EN_ROUTE);
        assertEquals(SlaRisk.UNKNOWN, engine.processGpsEvent(other).slaRisk());
    }

    @Test
    void gaugesCountVansByRisk() {
        plan(T0.plusSeconds(600), T0.plusSeconds(900));
        engine.processGpsEvent(gps(T0, 10, 36, 0, VanStatus.EN_ROUTE));
        assertEquals(1.0, registry.find("milkrun.vans.sla_risk").tag("level", "NONE").gauge().value());
        assertEquals(0.0, registry.find("milkrun.vans.sla_risk").tag("level", "CRITICAL").gauge().value());
        assertEquals(1.0, registry.find("milkrun.vans.tracked").gauge().value());
    }

    // ═══ Haversine helper ═══

    @Test
    void haversineDistanceSamePointShouldBeZero() {
        Location p = new Location(52.37, 4.90);
        assertEquals(0.0, EtaEngine.haversineDistance(p, p), 0.001);
    }

    @Test
    void haversineDistanceAmsterdamCentralToDamSquare() {
        double dist = EtaEngine.haversineDistance(new Location(52.3791, 4.9003), new Location(52.3730, 4.8932));
        assertTrue(dist > 0.5 && dist < 1.5, "got " + dist);
    }

    @Test
    void haversineDistanceShouldBeSymmetric() {
        Location a = new Location(52.37, 4.90);
        Location b = new Location(52.38, 4.92);
        assertEquals(EtaEngine.haversineDistance(a, b), EtaEngine.haversineDistance(b, a), 0.001);
    }

    @Test
    void haversineDistanceLongerDistances() {
        double dist = EtaEngine.haversineDistance(new Location(52.3676, 4.9041), new Location(51.9244, 4.4777));
        assertTrue(dist > 50 && dist < 65, "Amsterdam-Rotterdam got " + dist);
    }
}
