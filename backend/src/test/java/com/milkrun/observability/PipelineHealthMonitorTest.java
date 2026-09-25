package com.milkrun.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.*;

class PipelineHealthMonitorTest {

    private static final class ManualClock extends Clock {
        Instant now = Instant.parse("2026-09-25T00:00:00Z");

        void advance(Duration d) {
            now = now.plus(d);
        }

        @Override
        public Instant instant() {
            return now;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }
    }

    private SimpleMeterRegistry registry;
    private Counter received;
    private Counter deduplicated;
    private Counter processed;
    private ManualClock clock;
    private PipelineHealthMonitor monitor;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        received = registry.counter(PipelineHealthMonitor.RECEIVED);
        deduplicated = registry.counter(PipelineHealthMonitor.DEDUPLICATED);
        processed = registry.counter(PipelineHealthMonitor.PROCESSED);
        clock = new ManualClock();
        monitor = new PipelineHealthMonitor(registry, clock, Duration.ofMinutes(5), Duration.ofSeconds(15), 0.20);
    }

    /** Simulates traffic for the given duration, sampling every 15 s. */
    private void traffic(Duration duration, int receivedPerTick, int rejectedPerTick) {
        for (long s = 0; s < duration.toSeconds(); s += 15) {
            clock.advance(Duration.ofSeconds(15));
            received.increment(receivedPerTick);
            deduplicated.increment(rejectedPerTick);
            processed.increment(receivedPerTick - rejectedPerTick);
            monitor.sample();
        }
    }

    @Test
    void warmsUpBeforeFirstWindow() {
        monitor.sample();
        assertEquals(PipelineHealthMonitor.Status.WARMING_UP, monitor.report().status());
        traffic(Duration.ofMinutes(1), 1500, 100);
        assertEquals(PipelineHealthMonitor.Status.WARMING_UP, monitor.report().status());
    }

    @Test
    void healthyTrafficIsOk() {
        monitor.sample();
        traffic(Duration.ofMinutes(6), 1500, 100); // ~6.7% duplicates, like the simulator
        PipelineHealthMonitor.Report r = monitor.report();
        assertEquals(PipelineHealthMonitor.Status.OK, r.status(), r.problems().toString());
        assertEquals(6.7, r.rejectionRatePct(), 0.05);
        assertEquals(300, r.windowSeconds());
        assertEquals(93.3, r.processedPerSecond(), 0.05);
    }

    @Test
    void highRejectionRateInRecentWindowIsDegradedEvenWithHealthyHistory() {
        monitor.sample();
        traffic(Duration.ofHours(2), 1500, 100);  // long healthy history
        traffic(Duration.ofMinutes(5), 1500, 1485); // filter saturates: 99% rejected
        PipelineHealthMonitor.Report r = monitor.report();
        assertEquals(PipelineHealthMonitor.Status.DEGRADED, r.status());
        assertEquals(99.0, r.rejectionRatePct(), 0.05);
        assertTrue(r.problems().get(0).contains("dedup rejected"));
    }

    @Test
    void noTrafficForAFullWindowIsDegraded() {
        monitor.sample();
        traffic(Duration.ofMinutes(6), 0, 0);
        PipelineHealthMonitor.Report r = monitor.report();
        assertEquals(PipelineHealthMonitor.Status.DEGRADED, r.status());
        assertTrue(r.problems().get(0).contains("no GPS events"));
    }

    @Test
    void windowStaysBounded() {
        monitor.sample();
        traffic(Duration.ofHours(3), 10, 0);
        assertEquals(300, monitor.report().windowSeconds());
    }
}
