package com.milkrun.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.milkrun.engine.EtaEngine;
import com.milkrun.model.GpsEvent;
import com.milkrun.model.VanState;
import com.milkrun.persistence.GpsArchiveRepository;
import com.milkrun.pipeline.Deduplicator;
import com.milkrun.pipeline.IngestedGps;
import com.milkrun.pipeline.ReorderBuffer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * GPS event pipeline.
 *
 * <pre>
 * Kafka (gps-events) -> deserialize -> dedup -> reorder buffer        [Kafka consumer thread]
 * reorder buffer (every flush interval) -> ETA -> SSE sink -> archive  [single release thread]
 * </pre>
 *
 * Every record's offset is acknowledged only when the record has been dealt
 * with: after its state was published, when it is dropped as a duplicate,
 * once a malformed record has been forwarded to the dead-letter topic, or once
 * a late event has been reconciled into the archive and logged. Offsets are
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
    private final Deduplicator dedup;
    private final ReorderBuffer<IngestedGps> reorderBuffer;
    private final EtaEngine etaEngine;
    private final GpsArchiveRepository archive;
    private final LateEventHandler lateEvents;
    private final KafkaDeadLetterPublisher kafkaDeadLetters;
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
    private final Timer latency;
    private final Timer ingestLag;

    private Disposable ingestion;
    private Disposable release;

    public GpsEventPipeline(
            @Qualifier("gpsKafkaReceiver") KafkaReceiver<String, String> kafkaReceiver,
            ObjectMapper objectMapper,
            Deduplicator dedup,
            EtaEngine etaEngine,
            GpsArchiveRepository archive,
            LateEventHandler lateEvents,
            KafkaDeadLetterPublisher kafkaDeadLetters,
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
        this.kafkaDeadLetters = kafkaDeadLetters;
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
        this.latency = Timer.builder("milkrun.pipeline.latency")
                .description("Device timestamp to van state published (includes the reorder grace window)")
                .publishPercentiles(0.5, 0.95, 0.99)
                .publishPercentileHistogram()
                .register(meterRegistry);
        this.ingestLag = Timer.builder("milkrun.pipeline.ingest_lag")
                .description("Device timestamp to the event reaching the backend (network, device buffering, Kafka)")
                .publishPercentiles(0.5, 0.95, 0.99)
                .publishPercentileHistogram()
                .register(meterRegistry);
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
            log.warn("Forwarding malformed GPS record at {} to the DLQ topic: {}", record.receiverOffset(), e.getMessage());
            kafkaDeadLetters.publish(record, e.getClass().getSimpleName() + ": " + e.getMessage())
                    .doFinally(signal -> record.receiverOffset().acknowledge())
                    .subscribe();
            return;
        }

        if (dedup.isDuplicate(event.vanId(), event.sequenceNumber())) {
            eventsDeduplicated.increment();
            record.receiverOffset().acknowledge();
            return;
        }

        ingestLag.record(nonNegative(Duration.between(event.deviceTimestamp(), Instant.now())));
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
                latency.record(nonNegative(item.age(Instant.now())));
                archive.offer(item.event());
            } catch (Exception e) {
                processingErrors.increment();
                log.warn("Failed to process GPS event of {}: {}", item.event().vanId(), e.toString());
            } finally {
                item.ack();
            }
        }
    }

    private static Duration nonNegative(Duration d) {
        return d.isNegative() ? Duration.ZERO : d;
    }

    /** Recent end-to-end and ingest latency percentiles in milliseconds, for the health endpoint. */
    public Map<String, Object> latencySummary() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("end_to_end_ms", percentiles(latency));
        out.put("ingest_ms", percentiles(ingestLag));
        return out;
    }

    private static Map<String, Long> percentiles(Timer timer) {
        Map<String, Long> p = new LinkedHashMap<>();
        for (ValueAtPercentile v : timer.takeSnapshot().percentileValues()) {
            p.put("p" + Math.round(v.percentile() * 100), Math.round(v.value(TimeUnit.MILLISECONDS)));
        }
        return p;
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
