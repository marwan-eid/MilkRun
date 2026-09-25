package com.milkrun.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.milkrun.model.Location;
import com.milkrun.model.RoutePlan;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RoutePlanStoreTest {

    private SimpleMeterRegistry registry;
    private RoutePlanStore store;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        store = new RoutePlanStore(new ObjectMapper().findAndRegisterModules(),
                GeofenceDetector.withZones(List.of()), null, registry, "unused", "route-plans");
    }

    private static RoutePlan plan(String route, int version, Instant created, int stopVertex) {
        Instant deadline = created.plusSeconds(600);
        return new RoutePlan(route, "van-001", version, created, 4, 25,
                List.of(new RoutePlan.Stop(0, "c0", new Location(52.301, 4.85), deadline, deadline, 1, stopVertex)),
                RouteGeometryTest.northLine(5));
    }

    @Test
    void parsesTheSimulatorsMessageFormat() {
        String json = """
                {"route_id":"route-2026-09-25-van-001-1","van_id":"van-001","version":2,
                 "created_at":"2026-09-25T10:00:00.000Z","time_scale":4,"base_speed_kmh":25,
                 "stops":[{"stop_index":0,"customer_id":"cust-001-00","location":{"latitude":52.3005,"longitude":4.85},
                           "sla_deadline":"2026-09-25T10:05:00.000Z","planned_arrival":"2026-09-25T10:02:00.000Z",
                           "parcels":3,"waypoint_index":1}],
                 "waypoints":[[52.3,4.85],[52.3005,4.85],[52.301,4.85]]}""";
        store.accept(json).block();

        RoutePlanStore.PlannedRoute planned = store.get("van-001", "route-2026-09-25-van-001-1").orElseThrow();
        assertEquals(2, planned.plan().version());
        assertEquals(4.0, planned.plan().timeScale());
        assertEquals(Instant.parse("2026-09-25T10:05:00Z"), planned.plan().stops().get(0).slaDeadline());
        assertEquals(1, planned.plan().stops().get(0).waypointIndex());
        assertEquals(3, planned.geometry().vertexCount());
    }

    @Test
    void keepsTheNewestVersionOfARoute() {
        Instant t = Instant.parse("2026-09-25T10:00:00Z");
        assertNotNull(store.put(plan("r1", 1, t, 2)));
        assertNull(store.put(plan("r1", 0, t, 3)), "older version of the same route");
        assertEquals(1, store.get("van-001", "r1").orElseThrow().plan().version());
        assertNotNull(store.put(plan("r1", 2, t, 3)));
        assertEquals(2, store.get("van-001", "r1").orElseThrow().plan().version());
    }

    @Test
    void aNewerRouteReplacesTheOldOne() {
        Instant t = Instant.parse("2026-09-25T10:00:00Z");
        store.put(plan("r1", 3, t, 2));
        assertNotNull(store.put(plan("r2", 0, t.plusSeconds(900), 2)));
        assertTrue(store.get("van-001", "r1").isEmpty());
        assertTrue(store.get("van-001", "r2").isPresent());
        assertNull(store.put(plan("r1", 4, t, 2)), "a late record of the previous route is ignored");
    }

    @Test
    void rejectsMalformedPlans() {
        Instant t = Instant.parse("2026-09-25T10:00:00Z");
        assertNull(store.put(plan("r1", 0, t, 99)), "stop vertex outside the polyline");
        store.accept("{not json").block();
        assertTrue(store.get("van-001", "r1").isEmpty());
        assertEquals(2.0, registry.find("milkrun.route_plans.rejected").counter().count());
    }

    @Test
    void rebuildsGeometryWhenZonesChange() {
        GeofenceDetector zones = GeofenceDetector.withZones(List.of());
        RoutePlanStore s = new RoutePlanStore(new ObjectMapper().findAndRegisterModules(), zones, null,
                new SimpleMeterRegistry(), "unused", "route-plans");
        s.put(plan("r1", 0, Instant.now(), 2));
        assertEquals(1.0, s.get("van-001", "r1").orElseThrow().geometry().factorAt(10));

        zones.replaceZones(List.of(GeofenceDetector.Zone.rectangle("z", "Slow", 0.5, 4.8, 52.2, 4.9, 52.4)));
        assertEquals(0.5, s.get("van-001", "r1").orElseThrow().geometry().factorAt(10));
    }
}
