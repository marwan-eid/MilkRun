package com.milkrun.pipeline;

import com.milkrun.model.GpsEvent;
import com.milkrun.model.Location;
import com.milkrun.model.VanStatus;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ReorderBufferTest {

    private GpsEvent makeEvent(String vanId, long seqNum, Instant timestamp) {
        return new GpsEvent(
                UUID.randomUUID(), vanId, seqNum, timestamp, Instant.now(),
                new Location(52.37, 4.90), 25.0, 90.0, 80,
                "route-test", 0, 10, VanStatus.EN_ROUTE);
    }

    @Test
    void shouldBufferEventsWithinGraceWindow() {
        ReorderBuffer buffer = new ReorderBuffer(5000, 50); // 5s grace
        Instant now = Instant.now();

        // Events within grace window should not be flushed immediately
        List<GpsEvent> result = buffer.addAndFlush(makeEvent("van-001", 1, now));
        assertTrue(result.isEmpty(), "Events within grace window should be buffered");
    }

    @Test
    void shouldFlushEventsAfterGraceWindow() {
        ReorderBuffer buffer = new ReorderBuffer(100, 50); // 100ms grace

        // Add events with old timestamps (well past grace window)
        Instant past = Instant.now().minusSeconds(5);
        GpsEvent event1 = makeEvent("van-001", 1, past);
        GpsEvent event2 = makeEvent("van-001", 2, past.plusMillis(50));

        // Collect all flushed events across both calls
        List<GpsEvent> allFlushed = new ArrayList<>();
        allFlushed.addAll(buffer.addAndFlush(event1));
        allFlushed.addAll(buffer.addAndFlush(event2));

        // Both events should have been flushed (they're past the grace window)
        assertEquals(2, allFlushed.size(), "Events past grace window should be flushed");
        // Should be in chronological order
        assertTrue(allFlushed.get(0).sequenceNumber() <= allFlushed.get(1).sequenceNumber());
    }

    @Test
    void shouldReorderOutOfOrderEvents() {
        // Use a grace window that's long enough to hold all events,
        // then trigger flush via buffer overflow
        ReorderBuffer buffer = new ReorderBuffer(60000, 3); // large grace, tiny buffer

        Instant now = Instant.now();

        // Send events out of order — all with fresh timestamps within grace
        GpsEvent event3 = makeEvent("van-001", 3, now.plusMillis(200));
        GpsEvent event1 = makeEvent("van-001", 1, now);
        GpsEvent event2 = makeEvent("van-001", 2, now.plusMillis(100));

        List<GpsEvent> allFlushed = new ArrayList<>();
        allFlushed.addAll(buffer.addAndFlush(event3)); // buffered (1/3)
        allFlushed.addAll(buffer.addAndFlush(event1)); // buffered (2/3)
        allFlushed.addAll(buffer.addAndFlush(event2)); // buffered (3/3) — no overflow yet

        // Add one more event to trigger overflow flush (buffer max = 3, now 4th
        // triggers)
        GpsEvent event4 = makeEvent("van-001", 4, now.plusMillis(300));
        allFlushed.addAll(buffer.addAndFlush(event4)); // overflow → force flush

        // All events should have been flushed in timestamp order
        assertTrue(allFlushed.size() >= 3, "Should have flushed at least 3 events, got: " + allFlushed.size());
        // Verify chronological ordering
        for (int i = 1; i < allFlushed.size(); i++) {
            assertTrue(
                    allFlushed.get(i).deviceTimestamp().compareTo(allFlushed.get(i - 1).deviceTimestamp()) >= 0,
                    "Events should be in chronological order");
        }
    }

    @Test
    void shouldForceFlushOnBufferOverflow() {
        ReorderBuffer buffer = new ReorderBuffer(60000, 3); // large grace, small buffer

        Instant now = Instant.now();
        List<GpsEvent> allFlushed = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            List<GpsEvent> flushed = buffer.addAndFlush(
                    makeEvent("van-001", i, now.plusMillis(i * 100)));
            allFlushed.addAll(flushed);
        }

        // Buffer should force-flush when exceeding maxSize
        assertFalse(allFlushed.isEmpty(), "Should force-flush on overflow");
    }

    @Test
    void shouldIsolateVanBuffers() {
        ReorderBuffer buffer = new ReorderBuffer(100, 50);
        Instant past = Instant.now().minusSeconds(5);

        // Add events for two different vans — these will flush immediately (past grace)
        List<GpsEvent> van1First = buffer.addAndFlush(makeEvent("van-001", 1, past));
        List<GpsEvent> van2First = buffer.addAndFlush(makeEvent("van-002", 1, past));

        // Each van's events should flush independently
        assertEquals(1, van1First.size(), "Van-001 event should flush");
        assertEquals(1, van2First.size(), "Van-002 event should flush");
        assertEquals("van-001", van1First.get(0).vanId());
        assertEquals("van-002", van2First.get(0).vanId());
    }
}
