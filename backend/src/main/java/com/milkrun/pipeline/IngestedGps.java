package com.milkrun.pipeline;

import com.milkrun.model.GpsEvent;
import com.milkrun.model.VanStatus;
import reactor.kafka.receiver.ReceiverOffset;

import java.time.Duration;
import java.time.Instant;

/**
 * A GPS event together with the Kafka offset it came from. The offset is
 * acknowledged once the event has been fully handled (shown, dropped as a
 * duplicate, or dead-lettered), so a crash never loses an event whose offset
 * was already committed.
 */
public record IngestedGps(GpsEvent event, ReceiverOffset offset) {

    public void ack() {
        if (offset != null) {
            offset.acknowledge();
        }
    }

    /** Reorder buffer for ingested events: per van, by device time then sequence number. */
    public static ReorderBuffer<IngestedGps> reorderBuffer(Duration grace, int maxBufferSize) {
        return new ReorderBuffer<>(grace, maxBufferSize,
                i -> i.event().vanId(),
                i -> i.event().deviceTimestamp(),
                i -> i.event().sequenceNumber(),
                i -> i.event().status() == VanStatus.RETURNED);
    }

    /** Time from the device reading to now. */
    public Duration age(Instant now) {
        return Duration.between(event.deviceTimestamp(), now);
    }
}
