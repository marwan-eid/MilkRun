package com.milkrun.pipeline;

import com.milkrun.model.GpsEvent;
import com.milkrun.model.Location;
import com.milkrun.model.VanStatus;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ReorderBufferTest {

    private static final Instant T0 = Instant.parse("2026-09-25T10:00:00Z");

    private static ReorderBuffer<GpsEvent> buffer(long graceMs, int maxSize) {
        return new ReorderBuffer<>(Duration.ofMillis(graceMs), maxSize,
                GpsEvent::vanId, GpsEvent::deviceTimestamp, GpsEvent::sequenceNumber,
                e -> e.status() == VanStatus.RETURNED);
    }

    private static GpsEvent event(String vanId, long seq, Instant at) {
        return event(vanId, seq, at, VanStatus.EN_ROUTE);
    }

    private static GpsEvent event(String vanId, long seq, Instant at, VanStatus status) {
        return new GpsEvent(UUID.randomUUID(), vanId, seq, at, at,
                new Location(52.37, 4.90), 25.0, 90.0, 80, "route-test", 0, 10, status);
    }

    private static List<Long> seqs(List<GpsEvent> events) {
        return events.stream().map(GpsEvent::sequenceNumber).toList();
    }

    @Test
    void holdsEventsInsideTheGraceWindow() {
        ReorderBuffer<GpsEvent> buffer = buffer(3000, 50);
        buffer.offer(event("van-001", 1, T0));
        assertTrue(buffer.drainReady(T0.plusMillis(2999)).isEmpty());
        assertEquals(1, buffer.size());
    }

    @Test
    void releasesEventsInTimestampOrderOnceTheGraceWindowPasses() {
        ReorderBuffer<GpsEvent> buffer = buffer(3000, 50);
        buffer.offer(event("van-001", 3, T0.plusMillis(1000)));
        buffer.offer(event("van-001", 1, T0));
        buffer.offer(event("van-001", 2, T0.plusMillis(500)));

        assertEquals(List.of(1L, 2L), seqs(buffer.drainReady(T0.plusMillis(3600))));
        assertEquals(List.of(3L), seqs(buffer.drainReady(T0.plusMillis(4100))));
        assertEquals(0, buffer.size());
    }

    @Test
    void rejectsEventsOlderThanOneAlreadyReleased() {
        ReorderBuffer<GpsEvent> buffer = buffer(1000, 50);
        buffer.offer(event("van-001", 5, T0.plusMillis(500)));
        assertEquals(1, buffer.drainReady(T0.plusSeconds(2)).size());

        assertFalse(buffer.offer(event("van-001", 4, T0)), "older than what was already released");
        assertTrue(buffer.offer(event("van-001", 6, T0.plusMillis(600))));
        assertTrue(buffer.offer(event("van-002", 1, T0)), "other vans are unaffected");
    }

    @Test
    void terminalEventReleasesTheVanImmediately() {
        ReorderBuffer<GpsEvent> buffer = buffer(60_000, 50);
        buffer.offer(event("van-001", 2, T0.plusMillis(100)));
        buffer.offer(event("van-001", 3, T0.plusMillis(200), VanStatus.RETURNED));
        buffer.offer(event("van-001", 1, T0));
        buffer.offer(event("van-002", 1, T0));

        assertEquals(List.of(1L, 2L, 3L), seqs(buffer.drainReady(T0.plusMillis(300))));
        assertEquals(1, buffer.size(), "van-002 still waits for its grace window");
    }

    @Test
    void overflowReleasesTheVanImmediately() {
        ReorderBuffer<GpsEvent> buffer = buffer(60_000, 3);
        for (int i = 4; i >= 1; i--) {
            buffer.offer(event("van-001", i, T0.plusMillis(i * 100L)));
        }
        assertEquals(List.of(1L, 2L, 3L, 4L), seqs(buffer.drainReady(T0.plusMillis(500))));
    }

    @Test
    void sameTimestampIsOrderedBySequence() {
        ReorderBuffer<GpsEvent> buffer = buffer(100, 50);
        buffer.offer(event("van-001", 8, T0));
        buffer.offer(event("van-001", 7, T0));
        assertEquals(List.of(7L, 8L), seqs(buffer.drainReady(T0.plusSeconds(1))));
    }

    @Test
    void isolatesVans() {
        ReorderBuffer<GpsEvent> buffer = buffer(100, 50);
        buffer.offer(event("van-001", 1, T0));
        buffer.offer(event("van-002", 1, T0.plusSeconds(10)));
        List<GpsEvent> out = buffer.drainReady(T0.plusSeconds(1));
        assertEquals(1, out.size());
        assertEquals("van-001", out.get(0).vanId());
    }
}
