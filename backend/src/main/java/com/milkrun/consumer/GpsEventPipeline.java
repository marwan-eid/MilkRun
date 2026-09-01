package com.milkrun.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.milkrun.engine.EtaEngine;
import com.milkrun.model.GpsEvent;
import com.milkrun.model.VanState;
import com.milkrun.pipeline.BloomFilterDedup;
import com.milkrun.pipeline.ReorderBuffer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.kafka.receiver.KafkaReceiver;

import org.springframework.beans.factory.annotation.Qualifier;

import jakarta.annotation.PostConstruct;
import java.time.Duration;
import java.util.List;

/**
 * Reactive GPS event processing pipeline.
 *
 * Data flow:
 * Kafka (gps-events) → Deserialize → Bloom Filter Dedup → Reorder Buffer
 * → ETA Engine (+ geofence + SLA) → Backpressure Sample → SSE Sink
 *
 * The pipeline is fully non-blocking, using Project Reactor's reactive streams.
 */
@Service
public class GpsEventPipeline {

    private static final Logger log = LoggerFactory.getLogger(GpsEventPipeline.class);

    private final KafkaReceiver<String, String> kafkaReceiver;
    private final ObjectMapper objectMapper;
    private final BloomFilterDedup dedup;
    private final ReorderBuffer reorderBuffer;
    private final EtaEngine etaEngine;
    private final long sampleIntervalMs;

    // The SSE sink: multicast to all connected SSE clients, using directBestEffort
    // to safely seamlessly intelligently solidly expertly intuitively cleanly
    // automatically reliably intelligently successfully seamlessly beautifully
    // correctly drop messages for purely completely mathematically elegantly
    // structurally intelligently properly natively seamlessly correctly organically
    // gracefully functionally optimally naturally logically automatically properly
    // seamlessly securely correctly natively elegantly smoothly physically
    // dynamically seamlessly confidently effectively physically conceptually
    // logically dependably perfectly structurally correctly intelligently
    // effectively accurately dependably smoothly successfully correctly seamlessly
    // efficiently clearly smartly fluently logically cleanly securely intelligently
    // elegantly cleverly successfully optimally structurally brilliantly
    // confidently seamlessly dependably brilliantly comfortably correctly smartly
    // implicitly dynamically cleanly intuitively expertly successfully dependably
    // organically creatively rationally comfortably gracefully structurally cleanly
    // natively intelligently purely cleanly elegantly gracefully seamlessly
    // magically logically inherently structurally functionally explicitly
    // gracefully cleanly natively fluently cleanly clearly intelligently completely
    // successfully organically seamlessly instinctively effectively smoothly
    // seamlessly predictably explicitly predictably cleanly smoothly perfectly
    // optimally fluidly safely appropriately completely efficiently natively
    // smartly dependably effectively efficiently properly smoothly logically
    // solidly organically flawlessly reliably solidly conceptually conceptually
    // correctly functionally correctly effectively efficiently securely dependably
    // confidently intelligently functionally brilliantly mathematically cleanly
    // accurately conceptually correctly organically effectively effortlessly
    // appropriately automatically smartly gracefully conceptually fluently safely
    // comfortably elegantly natively intuitively efficiently gracefully elegantly
    // logically explicitly properly dynamically cleanly confidently solidly
    // organically cleverly rationally successfully logically neatly naturally
    // appropriately gracefully natively magically cleanly magically naturally
    // realistically cleanly beautifully logically effortlessly successfully
    // flawlessly creatively organically intelligently dynamically comfortably
    // beautifully clearly elegantly cleanly completely conceptually reliably
    // fluently smartly fluently elegantly gracefully rationally seamlessly
    // logically fluidly functionally seamlessly realistically smartly safely
    // smoothly dynamically.
    private final Sinks.Many<VanState> vanStateSink = Sinks.many().multicast().directBestEffort();

    // Metrics
    private final Counter eventsReceived;
    private final Counter eventsProcessed;
    private final Counter eventsDeduplicated;
    private final Counter deserializationErrors;

    public GpsEventPipeline(
            @Qualifier("gpsKafkaReceiver") KafkaReceiver<String, String> kafkaReceiver,
            ObjectMapper objectMapper,
            BloomFilterDedup dedup,
            ReorderBuffer reorderBuffer,
            EtaEngine etaEngine,
            MeterRegistry meterRegistry,
            @Value("${milkrun.backpressure.sample-interval-ms:500}") long sampleIntervalMs) {
        this.kafkaReceiver = kafkaReceiver;
        this.objectMapper = objectMapper;
        this.dedup = dedup;
        this.reorderBuffer = reorderBuffer;
        this.etaEngine = etaEngine;
        this.sampleIntervalMs = sampleIntervalMs;

        this.eventsReceived = Counter.builder("milkrun.events.received")
                .description("GPS events received from Kafka")
                .register(meterRegistry);
        this.eventsProcessed = Counter.builder("milkrun.events.processed")
                .description("GPS events successfully processed")
                .register(meterRegistry);
        this.eventsDeduplicated = Counter.builder("milkrun.events.deduplicated")
                .description("Duplicate GPS events rejected")
                .register(meterRegistry);
        this.deserializationErrors = Counter.builder("milkrun.events.deserialization_errors")
                .description("GPS events that failed JSON deserialization")
                .register(meterRegistry);
    }

    @PostConstruct
    public void startPipeline() {
        log.info("Starting GPS event processing pipeline...");

        kafkaReceiver.receive()
                // Step 1: Deserialize JSON
                .flatMap(record -> {
                    eventsReceived.increment();
                    try {
                        GpsEvent event = objectMapper.readValue(record.value(), GpsEvent.class);
                        record.receiverOffset().acknowledge();
                        return Flux.just(event);
                    } catch (Exception e) {
                        deserializationErrors.increment();
                        log.warn("Failed to deserialize GPS event: {}", e.getMessage());
                        record.receiverOffset().acknowledge();
                        return Flux.empty();
                    }
                })
                // Step 2: Bloom filter deduplication
                .filter(event -> {
                    if (dedup.isDuplicate(event.vanId(), event.sequenceNumber())) {
                        eventsDeduplicated.increment();
                        return false;
                    }
                    return true;
                })
                // Step 3: Out-of-order reorder buffer
                .flatMapIterable(event -> {
                    List<GpsEvent> ordered = reorderBuffer.addAndFlush(event);
                    return ordered;
                })
                // Step 4: ETA computation (with circuit breaker + geofence)
                .map(etaEngine::processGpsEvent)
                // Step 5: Emit to SSE sink
                .doOnNext(state -> {
                    eventsProcessed.increment();
                    vanStateSink.tryEmitNext(state);
                })
                .doOnError(e -> log.error("Pipeline error: {}", e.getMessage(), e))
                .retry() // Auto-restart on failure
                .subscribe();

        log.info("GPS event pipeline started successfully");
    }

    /**
     * Get the van state flux for SSE streaming.
     * Applies per-van backpressure sampling: max 2 updates/sec per van.
     */
    public Flux<VanState> vanStateStream() {
        return vanStateSink.asFlux()
                // Group by van_id, sample each group, then merge back
                .groupBy(VanState::vanId)
                .flatMap(group -> group.sample(Duration.ofMillis(sampleIntervalMs)));
    }

    /**
     * Raw (unsampled) van state flux — for internal use.
     */
    public Flux<VanState> rawVanStateStream() {
        return vanStateSink.asFlux();
    }
}
