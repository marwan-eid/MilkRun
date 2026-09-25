package com.milkrun.dispatch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.milkrun.engine.EtaEngine;
import com.milkrun.engine.GeofenceDetector;
import com.milkrun.engine.RoutePlanStore;
import com.milkrun.fleet.FleetView;
import com.milkrun.model.GpsEvent;
import com.milkrun.model.Location;
import com.milkrun.model.RoutePlan;
import com.milkrun.model.VanState;
import com.milkrun.model.VanStatus;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class DispatchServiceTest {

    private static final Instant T0 = Instant.parse("2026-09-25T10:00:00Z");
    /** Base 36 km/h simulated x time scale 4: vans cover 40 m of road per wall-clock second. */
    private static final double SPEED = 40;

    private RoutePlanStore store;
    private EtaEngine eta;
    private DispatchService dispatch;

    @BeforeEach
    void setUp() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        GeofenceDetector zones = GeofenceDetector.withZones(List.of());
        store = new RoutePlanStore(new ObjectMapper().findAndRegisterModules(), zones, null, registry, "x", "route-plans");
        eta = new EtaEngine(CircuitBreaker.ofDefaults("eta"), zones, store, registry, 60, 0, 1.3, 200);
        FleetView fleet = new FleetView() {
            public Collection<VanState> all() { return eta.getAllVanStates().values(); }
            public Optional<VanState> get(String id) { return Optional.ofNullable(eta.getAllVanStates().get(id)); }
            public Flux<VanState> updates() { return Flux.empty(); }
        };
        dispatch = new DispatchService(store, eta, fleet, Clock.fixed(T0, ZoneOffset.UTC), 1.3, 1200, 40, 5);
    }

    /**
     * Van driving north along {@code lon}: hub at 52.300, stops at 52.310 and 52.315,
     * return point at 52.320 (vertices every 0.0005°, ~55.6 m).
     */
    private void van(String van, double lon, Instant deadline1) {
        double[][] w = new double[41][];
        for (int i = 0; i < w.length; i++) {
            w[i] = new double[] { 52.300 + i * 0.0005, lon };
        }
        // Planned arrivals match what the van will actually do, so it runs on time
        Instant plannedStop0 = T0.plusSeconds(Math.round(10 * 55.6 / SPEED));
        Instant plannedStop1 = plannedStop0.plusSeconds(10 + Math.round(10 * 55.6 / SPEED));
        store.put(new RoutePlan("route-" + van, van, 0, T0, 4, 36, List.of(
                new RoutePlan.Stop(0, "c0", new Location(52.310, lon), T0.plusSeconds(600), plannedStop0, 1, 20),
                new RoutePlan.Stop(1, "c1", new Location(52.315, lon), deadline1, plannedStop1, 1, 30)), w));
    }

    private void ping(String van, double lon, int vertex, int stopIndex, VanStatus status) {
        eta.processGpsEvent(new GpsEvent(UUID.randomUUID(), van, 1, T0, T0,
                new Location(52.300 + vertex * 0.0005, lon), status == VanStatus.DELIVERING ? 0 : 36, 0, 90,
                "route-" + van, stopIndex, 2, status));
    }

    private Optional<DispatchPlanner.Assignment> order(double lat, double lon) {
        return dispatch.assign(new DispatchPlanner.Order("o-1", new Location(lat, lon), T0));
    }

    @Test
    void picksTheVanAndPositionWithTheShortestDetour() {
        van("van-a", 4.850, T0.plusSeconds(900));
        van("van-b", 4.900, T0.plusSeconds(900)); // ~3.4 km east
        ping("van-a", 4.850, 10, 0, VanStatus.EN_ROUTE);
        ping("van-b", 4.900, 10, 0, VanStatus.EN_ROUTE);

        // Just east of van-a's route, between its two stops
        DispatchPlanner.Assignment a = order(52.3125, 4.851).orElseThrow();
        assertEquals("van-a", a.vanId());
        assertEquals(1, a.insertBefore(), "between stop 0 and stop 1");
        assertEquals(3, a.stopsAfterInsert());
        assertTrue(a.extraKm() < 0.1, "tiny detour, got " + a.extraKm());
        assertTrue(a.onTime());
        assertEquals(0, a.stopsMadeLate());
        assertEquals(6, a.optionsConsidered(), "3 positions x 2 vans");
    }

    @Test
    void avoidsAnInsertionThatMakesAnotherStopLate() {
        Instant tight = T0.plusSeconds(Math.round(10 * 55.6 / SPEED)).plusSeconds(10 + Math.round(10 * 55.6 / SPEED) + 5);
        van("van-a", 4.850, tight);                  // stop 1 has 5 s of slack
        van("van-b", 4.852, T0.plusSeconds(900));    // same distance to the order, plenty of slack
        ping("van-a", 4.850, 10, 0, VanStatus.EN_ROUTE);
        ping("van-b", 4.852, 10, 0, VanStatus.EN_ROUTE);

        DispatchPlanner.Assignment a = order(52.3125, 4.851).orElseThrow();
        assertEquals("van-b", a.vanId());
        assertEquals(0, a.stopsMadeLate());
    }

    @Test
    void aDeliveringVanTakesTheOrderAfterTheStopItIsServing() {
        van("van-a", 4.850, T0.plusSeconds(900));
        ping("van-a", 4.850, 20, 0, VanStatus.DELIVERING);

        // Right next to stop 0: the cheapest place would be before stop 0, but the van is already there
        DispatchPlanner.Assignment a = order(52.3095, 4.8505).orElseThrow();
        assertTrue(a.insertBefore() >= 1, "got " + a.insertBefore());
    }

    @Test
    void stillAssignsWhenNoVanCanMakeTheSlot() {
        van("van-a", 4.850, T0.plusSeconds(900));
        ping("van-a", 4.850, 10, 0, VanStatus.EN_ROUTE);

        // ~65 km away (Rotterdam): at 40 m/s that's ~30 min, the 20-simulated-minute slot is 5 min
        DispatchPlanner.Assignment a = order(51.92, 4.48).orElseThrow();
        assertFalse(a.onTime());
        assertEquals("van-a", a.vanId());
    }

    @Test
    void noAssignmentWithoutVansOnTheirRoutes() {
        van("van-a", 4.850, T0.plusSeconds(900));
        ping("van-a", 4.850, 35, 2, VanStatus.RETURNING);
        ping("van-z", 4.860, 10, 0, VanStatus.EN_ROUTE); // no plan for this van
        assertTrue(order(52.3125, 4.851).isEmpty());
    }

    @Test
    void predictedArrivalAndSlotAreWallClock() {
        van("van-a", 4.850, T0.plusSeconds(900));
        ping("van-a", 4.850, 10, 0, VanStatus.EN_ROUTE);
        DispatchPlanner.Assignment a = order(52.3125, 4.851).orElseThrow();
        // 1200 simulated seconds / time scale 4
        assertEquals(T0.plusSeconds(300), a.slaDeadline());
        assertTrue(a.predictedArrival().isAfter(T0));
        assertTrue(a.predictedArrival().isBefore(a.slaDeadline()));
    }
}
