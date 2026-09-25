package com.milkrun.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.milkrun.engine.EtaEngine;
import com.milkrun.model.GpsEvent;
import com.milkrun.model.VanState;
import com.milkrun.persistence.GpsArchiveRepository;
import com.milkrun.pipeline.BloomFilterDedup;
import com.milkrun.pipeline.IngestedGps;
import com.milkrun.pipeline.ReorderBuffer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.Instant;

/**
 * GPS event pipeline.
 *
 * <pre>
 * Kafka (gps-events) -> deserialize -> dedup -> reorder buffer        [Kafka consumer thread]
 * reorder buffer (every flush interval) -> ETA -> SSE sink -> archive  [single release thread]
 * </pre>
 *
 * Every record's offset is acknowledged only when the record has been dealt
 * with: after its state was published, or when it is dropped as a duplicate
 * or malformed, or once a late event has been dead-lettered. Offsets are
 * acknowledged out of order (events wait in the buffer), so the receiver is
 * configured with deferred commits: a partition's committed offset only moves
 * past records that are done. After a crash the consumer resumes from there
 * and replays whatever was still in flight.
 *
 * One thread releases events, so the ETA stage and the SSE sink see each
 * van's events strictly in order.
 */
@Service
public class GpsEventPipeline {

    private static final Logger log = LoggerFactory.getLogger(GpsEventPipeline.class);

    private final KafkaReceiver<String, String> kafkaReceiver;
    private final ObjectMapper objectMapper;
    private final BloomFilterDedup dedup;
    private final ReorderBuffer<IngestedGps> reorderBuffer;
    private final EtaEngine etaEngine;
    private final GpsArchiveRepository archive;
    private final LateEventHandler lateEvents;
    private final long sampleIntervalMs;
    private final Duration releaseInterval;
    private final Scheduler releaseScheduler = Schedulers.newSingle("gps-release");

    // SSE fan-out: multicast to every connected client. directBestEffort drops
    // an update for a subscriber that has no outstanding demand (slow or sleeping
    // client) instead of buffering it or failing the whole sink.
    private final Sinks.Many<VanState> vanStateSink = Sinks.many().multicast().directBestEffort();

    private final Counter eventsReceived;
    private final Counter eventsProcessed;
    private final Counter eventsDeduplicated;
    private final Counter deserializationErrors;
    private final Counter eventsLate;
    private final Counter processingErrors;

    private Disposable ingestion;
    private Disposable release;

    public GpsEventPipeline(
            @Qualifier("gpsKafkaReceiver") KafkaReceiver<String, String> kafkaReceiver,
            ObjectMapper objectMapper,
            BloomFilterDedup dedup,
            EtaEngine etaEngine,
            GpsArchiveRepository archive,
            LateEventHandler lateEvents,
            MeterRegistry meterRegistry,
            @Value("${milkrun.pipeline.reorder-buffer-grace-ms:3000}") long graceMs,
            @Value("${milkrun.pipeline.reorder-buffer-max-size:50}") int maxBufferSize,
            @Value("${milkrun.pipeline.release-interval-ms:100}") long releaseIntervalMs,
            @Value("${milkrun.backpressure.sample-interval-ms:500}") long sampleIntervalMs) {
        this.kafkaReceiver = kafkaReceiver;
        this.objectMapper = objectMapper;
        this.dedup = dedup;
        this.reorderBuffer = IngestedGps.reorderBuffer(Duration.ofMillis(graceMs), maxBufferSize);
        this.etaEngine = etaEngine;
        this.archive = archive;
        this.lateEvents = lateEvents;
        this.releaseInterval = Duration.ofMillis(releaseIntervalMs);
        this.sampleIntervalMs = sampleIntervalMs;

        this.eventsReceived = Counter.builder("milkrun.events.received")
                .description("GPS events received from Kafka").register(meterRegistry);
        this.eventsProcessed = Counter.builder("milkrun.events.processed")
                .description("GPS events released in order and turned into van state").register(meterRegistry);
        this.eventsDeduplicated = Counter.builder("milkrun.events.deduplicated")
                .description("Duplicate GPS events rejected").register(meterRegistry);
        this.deserializationErrors = Counter.builder("milkrun.events.deserialization_errors")
                .description("GPS records that were not valid events").register(meterRegistry);
        this.eventsLate = Counter.builder("milkrun.events.late")
                .description("GPS events that arrived after newer ones were released").register(meterRegistry);
        this.processingErrors = Counter.builder("milkrun.events.processing_errors")
                .description("GPS events whose state could not be computed").register(meterRegistry);
        Gauge.builder("milkrun.reorder_buffer.size", reorderBuffer, ReorderBuffer::size)
                .description("GPS events waiting in the reorder buffer").register(meterRegistry);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void startPipeline() {
        ingestion = kafkaReceiver.receive()
                .doOnNext(this::ingest)
                .doOnError(e -> log.error("GPS consumer failed, restarting: {}", e.toString()))
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
                        .maxBackoff(Duration.ofSeconds(30))
                        .transientErrors(true))
                .subscribe();

        release = Flux.interval(releaseInterval, releaseScheduler)
                .onBackpressureDrop()
                .subscribe(tick -> releaseReady(Instant.now()));

        log.info("GPS event pipeline started");
    }

    @PreDestroy
    void stop() {
        if (ingestion != null) ingestion.dispose();
        if (release != null) release.dispose();
        releaseScheduler.dispose();
    }

    /** Runs on the Kafka consumer thread: parse, dedup, buffer. Never throws. */
    void ingest(ReceiverRecord<String, String> record) {
        eventsReceived.increment();
        GpsEvent event;
        try {
            event = objectMapper.readValue(record.value(), GpsEvent.class);
            String error = event.validationError();
            if (error != null) {
                throw new IllegalArgumentException(error);
            }
        } catch (Exception e) {
            deserializationErrors.increment();
            log.warn("Dropping malformed GPS record at {}: {}", record.receiverOffset(), e.getMessage());
            record.receiverOffset().acknowledge();
            return;
        }

        if (dedup.isDuplicate(event.vanId(), event.sequenceNumber())) {
            eventsDeduplicated.increment();
            record.receiverOffset().acknowledge();
            return;
        }

        IngestedGps item = new IngestedGps(event, record.receiverOffset());
        if (!reorderBuffer.offer(item)) {
            eventsLate.increment();
            lateEvents.handle(item).subscribe();
        }
    }

    /** Runs on the single release thread: every event past its grace window, in order. */
    void releaseReady(Instant now) {
        for (IngestedGps item : reorderBuffer.drainReady(now)) {
            try {
                VanState state = etaEngine.processGpsEvent(item.event());
                eventsProcessed.increment();
                vanStateSink.tryEmitNext(state);
                archive.offer(item.event());
            } catch (Exception e) {
                processingErrors.increment();
                log.warn("Failed to process GPS event of {}: {}", item.event().vanId(), e.toString());
            } finally {
                item.ack();
            }
        }
    }

    /**
     * Van states for SSE clients, sampled to at most one update per van per
     * sample interval.
     */
    public Flux<VanState> vanStateStream() {
        return vanStateSink.asFlux()
                .groupBy(VanState::vanId)
                .flatMap(group -> group.sample(Duration.ofMillis(sampleIntervalMs)));
    }

    /** Every van state as it is produced (unsampled). */
    public Flux<VanState> rawVanStateStream() {
        return vanStateSink.asFlux();
    }
}
