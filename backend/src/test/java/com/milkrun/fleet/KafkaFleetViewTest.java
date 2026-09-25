package com.milkrun.fleet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.milkrun.model.Location;
import com.milkrun.model.VanState;
import com.milkrun.model.VanState.DataConfidence;
import com.milkrun.model.VanState.EtaMethod;
import com.milkrun.model.VanState.SlaRisk;
import com.milkrun.model.VanStatus;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

class KafkaFleetViewTest {

    private static final Instant T0 = Instant.parse("2026-09-25T10:00:00Z");
    private final ObjectMapper json = new ObjectMapper().findAndRegisterModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    private KafkaFleetView view;

    @BeforeEach
    void setUp() {
        // Publishing and consuming are not started; only the replica logic is exercised.
        view = new KafkaFleetView(null, null, json, new SimpleMeterRegistry(), "unused", "van-state", 500);
    }

    private static VanState state(String van, Instant at, VanStatus status, double lat) {
        return new VanState(van, "r-" + van, new Location(lat, 4.9), 25, 0, 80, status, 1, 10, 60,
                SlaRisk.NONE, DataConfidence.REAL_TIME, false, null, at, at.plusSeconds(600), at.plusSeconds(60),
                540L, EtaMethod.ROUTE);
    }

    @Test
    void publishesAtMostOncePerIntervalPerVanUnlessTheStatusChanges() {
        assertTrue(view.due(state("v1", T0, VanStatus.EN_ROUTE, 52.30)), "first state");
        assertFalse(view.due(state("v1", T0.plusMillis(200), VanStatus.EN_ROUTE, 52.31)));
        assertTrue(view.due(state("v2", T0.plusMillis(200), VanStatus.EN_ROUTE, 52.31)), "other vans are independent");
        assertTrue(view.due(state("v1", T0.plusMillis(300), VanStatus.DELIVERING, 52.31)), "status changed");
        assertFalse(view.due(state("v1", T0.plusMillis(700), VanStatus.DELIVERING, 52.31)));
        assertTrue(view.due(state("v1", T0.plusMillis(800), VanStatus.DELIVERING, 52.31)), "interval elapsed");
    }

    @Test
    void replicaKeepsTheNewestStateOfEachVan() throws Exception {
        List<VanState> emitted = new CopyOnWriteArrayList<>();
        view.updates().subscribe(emitted::add);

        view.apply(json.writeValueAsString(state("v1", T0.plusSeconds(2), VanStatus.EN_ROUTE, 52.32)));
        view.apply(json.writeValueAsString(state("v1", T0.plusSeconds(1), VanStatus.EN_ROUTE, 52.31))); // older, e.g. during a rebalance
        view.apply(json.writeValueAsString(state("v2", T0, VanStatus.DELIVERING, 52.40)));
        view.apply("{not json");

        assertEquals(2, view.all().size());
        assertEquals(52.32, view.get("v1").orElseThrow().location().latitude());
        assertEquals(VanStatus.DELIVERING, view.get("v2").orElseThrow().status());
        assertEquals(2, emitted.size(), "the stale state is not re-broadcast");
        assertEquals(540L, view.get("v1").orElseThrow().slaSlackSeconds(), "all fields survive the round trip");
    }
}
