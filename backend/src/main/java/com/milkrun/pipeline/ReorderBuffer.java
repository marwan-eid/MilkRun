package com.milkrun.pipeline;

import com.milkrun.model.GpsEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-van out-of-order reconciliation buffer.
 *
 * GPS events from moving vans arrive out of order due to cellular network
 * jitter and Kafka partition rebalancing. This buffer holds events in a
 * time-windowed priority queue (keyed by device_timestamp) for a configurable
 * grace period before flushing them in correct chronological order.
 *
 * Events arriving after the grace window closes are emitted on a separate
 * "late events" flux for DLQ processing.
 */
@Component
public class ReorderBuffer {

    private static final Logger log = LoggerFactory.getLogger(ReorderBuffer.class);

    private final long graceMs;
    private final int maxBufferSize;
    private final ConcurrentHashMap<String, VanBuffer> vanBuffers = new ConcurrentHashMap<>();
    private final Sinks.Many<GpsEvent> lateEventsSink = Sinks.many().multicast().onBackpressureBuffer();

    public ReorderBuffer(
            @Value("${milkrun.pipeline.reorder-buffer-grace-ms:3000}") long graceMs,
            @Value("${milkrun.pipeline.reorder-buffer-max-size:50}") int maxBufferSize) {
        this.graceMs = graceMs;
        this.maxBufferSize = maxBufferSize;
        log.info("ReorderBuffer initialized: graceMs={}, maxBufferSize={}", graceMs, maxBufferSize);
    }

    /**
     * Add an event to the buffer and return any events ready to be flushed
     * (i.e., their grace window has expired).
     */
    public List<GpsEvent> addAndFlush(GpsEvent event) {
        VanBuffer buffer = vanBuffers.computeIfAbsent(event.vanId(), VanBuffer::new);
        return buffer.addAndFlush(event);
    }

    /**
     * Flux of late events that arrived after their grace window.
     * These should be sent to the DLQ.
     */
    public Flux<GpsEvent> lateEvents() {
        return lateEventsSink.asFlux();
    }

    /**
     * Per-van buffer backed by a PriorityQueue ordered by device_timestamp.
     */
    private class VanBuffer {
        private final String vanId;
        private final PriorityQueue<GpsEvent> queue;
        private Instant lastFlushedTimestamp = Instant.EPOCH;

        VanBuffer(String vanId) {
            this.vanId = vanId;
            this.queue = new PriorityQueue<>(
                    Comparator.comparing(GpsEvent::deviceTimestamp)
                            .thenComparingLong(GpsEvent::sequenceNumber));
        }

        synchronized List<GpsEvent> addAndFlush(GpsEvent event) {
            // Check if this event is "late" — its timestamp is before our last flushed
            // event
            if (event.deviceTimestamp().isBefore(lastFlushedTimestamp)) {
                log.debug("Late event detected: van={}, seq={}, deviceTs={}, lastFlushed={}",
                        vanId, event.sequenceNumber(), event.deviceTimestamp(), lastFlushedTimestamp);
                lateEventsSink.tryEmitNext(event);
                return Collections.emptyList();
            }

            queue.add(event);

            // Bypass grace window for terminal states — it guarantees no more events are
            // coming to push the buffer out
            if (com.milkrun.model.VanStatus.RETURNED.equals(event.status())) {
                log.info("Terminal RETURNED state received for van={}, force-flushing final buffer", vanId);
                return flushAll();
            }

            // Force flush if buffer is too large (backpressure safety)
            if (queue.size() > maxBufferSize) {
                log.warn("Buffer overflow for van={}, force-flushing {} events", vanId, queue.size());
                return flushAll();
            }

            return flushReady();
        }

        private List<GpsEvent> flushReady() {
            List<GpsEvent> ready = new ArrayList<>();
            Instant cutoff = Instant.now().minusMillis(graceMs);

            while (!queue.isEmpty() && queue.peek().deviceTimestamp().isBefore(cutoff)) {
                GpsEvent event = queue.poll();
                lastFlushedTimestamp = event.deviceTimestamp();
                ready.add(event);
            }

            return ready;
        }

        private List<GpsEvent> flushAll() {
            List<GpsEvent> all = new ArrayList<>();
            while (!queue.isEmpty()) {
                GpsEvent event = queue.poll();
                lastFlushedTimestamp = event.deviceTimestamp();
                all.add(event);
            }
            return all;
        }
    }
}
