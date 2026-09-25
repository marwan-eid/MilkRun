package com.milkrun.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.milkrun.engine.EtaEngine;
import com.milkrun.engine.GeofenceDetector;
import com.milkrun.model.DeliveryEvent;
import com.milkrun.persistence.DeadLetterRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/**
 * Persists delivery outcomes so the analytics layer can query them.
 *
 * Records of one partition are processed strictly one after another (a van's
 * events share a partition, so its stops are handled in order); partitions
 * run in parallel. A record's offset is acknowledged only after its writes
 * succeeded, and every write is idempotent (keyed by the event id, or a pure
 * function of the delivery log), so replaying records after a crash is safe.
 *
 * A record that keeps failing is retried with backoff and then moved to the
 * dead-letter log so it cannot block its partition; if even that write fails
 * (database down), the consumer restarts from the last committed offset.
 *
 * <pre>
 * ARRIVAL            -> route row if missing, delivery log row, SLA breach if late
 * DELIVERY_COMPLETED -> delivery log row, route counters
 * DELIVERY_FAILED    -> delivery log row, route counters
 * DEPARTURE          -> departure time; after the last stop, the route is completed
 * </pre>
 */
@Service
public class DeliveryEventPipeline {

    private static final Logger log = LoggerFactory.getLogger(DeliveryEventPipeline.class);
    /** Breaches longer than this (wall-clock seconds) are CRITICAL, shorter ones WARNING. */
    private static final long CRITICAL_BREACH_SECONDS = 120;

    private final KafkaReceiver<String, String> kafkaReceiver;
    private final ObjectMapper objectMapper;
    private final DatabaseClient db;
    private final GeofenceDetector geofenceDetector;
    private final EtaEngine etaEngine;
    private final DeadLetterRepository deadLetters;
    private final Retry writeRetry;

    private final Counter deliveryEventsReceived;
    private final Counter deliveryEventsProcessed;
    private final Counter deliveryEventsDeadLettered;
    private final Counter slaBreachesDetected;

    private Disposable subscription;

    public DeliveryEventPipeline(
            @Qualifier("deliveryKafkaReceiver") KafkaReceiver<String, String> kafkaReceiver,
            ObjectMapper objectMapper,
            DatabaseClient databaseClient,
            MeterRegistry meterRegistry,
            GeofenceDetector geofenceDetector,
            EtaEngine etaEngine,
            DeadLetterRepository deadLetters) {
        this.kafkaReceiver = kafkaReceiver;
        this.objectMapper = objectMapper;
        this.db = databaseClient;
        this.geofenceDetector = geofenceDetector;
        this.etaEngine = etaEngine;
        this.deadLetters = deadLetters;
        this.writeRetry = Retry.backoff(3, Duration.ofMillis(200)).maxBackoff(Duration.ofSeconds(2));

        this.deliveryEventsReceived = Counter.builder("milkrun.delivery.events.received")
                .description("Delivery events received from Kafka").register(meterRegistry);
        this.deliveryEventsProcessed = Counter.builder("milkrun.delivery.events.processed")
                .description("Delivery events persisted to the database").register(meterRegistry);
        this.deliveryEventsDeadLettered = Counter.builder("milkrun.delivery.events.dead_lettered")
                .description("Delivery records moved to the dead-letter log").register(meterRegistry);
        this.slaBreachesDetected = Counter.builder("milkrun.delivery.sla_breaches")
                .description("Stops where the van arrived after the delivery slot ended").register(meterRegistry);
    }

    /** Starts once the application is ready, i.e. after Flyway has migrated the schema. */
    @EventListener(ApplicationReadyEvent.class)
    public void startPipeline() {
        subscription = kafkaReceiver.receive()
                .groupBy(record -> record.receiverOffset().topicPartition())
                .flatMap(partition -> partition.concatMap(this::handleRecord))
                .doOnError(e -> log.error("Delivery consumer failed, restarting from committed offsets: {}", e.toString()))
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
                        .maxBackoff(Duration.ofSeconds(30))
                        .transientErrors(true))
                .subscribe();
        log.info("Delivery event pipeline started");
    }

    @PreDestroy
    void stop() {
        if (subscription != null) {
            subscription.dispose();
        }
    }

    /** Processes one record; acknowledges it once it is persisted or dead-lettered. */
    Mono<Void> handleRecord(ReceiverRecord<String, String> record) {
        deliveryEventsReceived.increment();
        DeliveryEvent event;
        try {
            event = objectMapper.readValue(record.value(), DeliveryEvent.class);
            String error = event.validationError();
            if (error != null) {
                throw new IllegalArgumentException(error);
            }
        } catch (Exception e) {
            return deadLetter(record, null, "MALFORMED: " + e.getMessage());
        }

        return process(event)
                .retryWhen(writeRetry)
                .doOnSuccess(v -> {
                    deliveryEventsProcessed.increment();
                    record.receiverOffset().acknowledge();
                })
                .onErrorResume(e -> deadLetter(record, event.vanId(),
                        "DB_WRITE_FAILED: " + rootMessage(e)));
    }

    private Mono<Void> deadLetter(ReceiverRecord<String, String> record, String vanId, String reason) {
        log.warn("Dead-lettering delivery record at {}: {}", record.receiverOffset(), reason);
        return deadLetters.logDeadLetter(record.topic(), vanId, record.value(), reason, false)
                .retryWhen(writeRetry)
                .doOnSuccess(v -> {
                    deliveryEventsDeadLettered.increment();
                    record.receiverOffset().acknowledge();
                });
    }

    Mono<Void> process(DeliveryEvent event) {
        return switch (event.eventType()) {
            case ARRIVAL -> handleArrival(event);
            case DELIVERY_COMPLETED -> insertDeliveryLog(event, "COMPLETED", false, 0).then(refreshCounters(event.routeId()));
            case DELIVERY_FAILED -> insertDeliveryLog(event, "FAILED", false, 0).then(refreshCounters(event.routeId()));
            case DEPARTURE -> handleDeparture(event);
        };
    }

    private Mono<Void> handleArrival(DeliveryEvent event) {
        long breachSeconds = event.slaDeadline() != null
                ? Duration.between(event.slaDeadline(), event.timestamp()).toSeconds()
                : 0;
        boolean breached = breachSeconds > 0;

        Mono<Void> breach = Mono.empty();
        if (breached) {
            GeofenceDetector.GeofenceResult zone = geofenceDetector.check(event.location());
            UUID zoneId = zone.inGeofence() ? UUID.fromString(zone.zoneId()) : null;
            Instant predicted = etaEngine.initialPrediction(event.vanId(), event.routeId(), event.customerId());
            String severity = breachSeconds > CRITICAL_BREACH_SECONDS ? "CRITICAL" : "WARNING";
            breach = insertSlaBreach(event, breachSeconds, severity, zoneId, predicted)
                    .doOnNext(inserted -> {
                        if (inserted > 0) slaBreachesDetected.increment();
                    })
                    .then();
        }

        return ensureRoute(event)
                .then(insertDeliveryLog(event, "IN_PROGRESS", breached, Math.max(0, breachSeconds)))
                .then(breach);
    }

    private Mono<Void> handleDeparture(DeliveryEvent event) {
        Mono<Void> departed = db.sql("""
                UPDATE delivery_logs SET actual_departure = :at
                WHERE route_id = :routeId AND customer_id = :customerId AND delivery_status = 'IN_PROGRESS'
                """)
                .bind("at", event.timestamp())
                .bind("routeId", event.routeId())
                .bind("customerId", event.customerId())
                .then();
        boolean lastStop = event.stopIndex() >= event.totalStops() - 1;
        return lastStop ? departed.then(completeRoute(event.routeId(), event.timestamp())) : departed;
    }

    // ═══════════════════ DB Operations (all idempotent) ═══════════════════

    /**
     * Creates the route row if the route plan has not done so yet (it normally
     * has). Placeholder planned times are replaced when the plan is recorded.
     */
    private Mono<Void> ensureRoute(DeliveryEvent event) {
        return db.sql("""
                INSERT INTO completed_routes (route_id, van_id, planned_start, planned_end, total_stops, status)
                VALUES (:routeId, :vanId, :at, :at, :totalStops, 'IN_PROGRESS')
                ON CONFLICT (route_id) DO NOTHING
                """)
                .bind("routeId", event.routeId())
                .bind("vanId", event.vanId())
                .bind("at", event.timestamp())
                .bind("totalStops", event.totalStops())
                .then();
    }

    /** Recomputes the route's counters from the delivery log. */
    private Mono<Void> refreshCounters(String routeId) {
        return db.sql("""
                UPDATE completed_routes r SET
                    completed_stops = (SELECT count(*) FROM delivery_logs d
                                       WHERE d.route_id = r.route_id AND d.delivery_status = 'COMPLETED'),
                    failed_stops = (SELECT count(*) FROM delivery_logs d
                                    WHERE d.route_id = r.route_id AND d.delivery_status = 'FAILED')
                WHERE r.route_id = :routeId
                """)
                .bind("routeId", routeId)
                .then();
    }

    /**
     * Marks the route completed after its last delivery. Distance is the length
     * of the archived GPS track from the hub to that stop (the planned distance
     * if there is no track); speed is in simulated km/h, i.e. distance over
     * wall-clock duration x time scale, stops included.
     */
    private Mono<Void> completeRoute(String routeId, Instant end) {
        Mono<Void> complete = db.sql("""
                UPDATE completed_routes r SET
                    status = 'COMPLETED',
                    actual_start = COALESCE(r.actual_start, r.planned_start),
                    actual_end = :end,
                    total_duration_min = EXTRACT(EPOCH FROM (CAST(:end AS timestamptz) - r.planned_start)) / 60.0,
                    completed_stops = (SELECT count(*) FROM delivery_logs d
                                       WHERE d.route_id = r.route_id AND d.delivery_status = 'COMPLETED'),
                    failed_stops = (SELECT count(*) FROM delivery_logs d
                                    WHERE d.route_id = r.route_id AND d.delivery_status = 'FAILED'),
                    total_distance_km = COALESCE(
                        (SELECT ST_Length(ST_MakeLine(a.location ORDER BY a.device_timestamp)::geography) / 1000
                         FROM gps_archive a WHERE a.route_id = r.route_id HAVING count(*) > 1),
                        r.planned_distance_km)
                WHERE r.route_id = :routeId AND r.status <> 'COMPLETED'
                """)
                .bind("end", end)
                .bind("routeId", routeId)
                .then();
        Mono<Void> speed = db.sql("""
                UPDATE completed_routes SET avg_speed_kmh = LEAST(999.99,
                    total_distance_km / NULLIF(total_duration_min / 60.0 * COALESCE(time_scale, 1), 0))
                WHERE route_id = :routeId
                """)
                .bind("routeId", routeId)
                .then();
        return complete.then(speed)
                .doOnSuccess(v -> log.info("Route {} completed", routeId));
    }

    private Mono<Void> insertDeliveryLog(DeliveryEvent event, String deliveryStatus, boolean breached, long breachSeconds) {
        return db.sql("""
                INSERT INTO delivery_logs (event_id, route_id, van_id, stop_index, customer_id, location,
                    sla_deadline, actual_arrival, parcels_delivered, delivery_status, sla_breached, breach_seconds)
                VALUES (:eventId, :routeId, :vanId, :stopIndex, :customerId,
                    ST_SetSRID(ST_MakePoint(:lon, :lat), 4326),
                    :slaDeadline, :arrival, :parcels, :status, :breached, :breachSeconds)
                ON CONFLICT (event_id) DO NOTHING
                """)
                .bind("eventId", event.eventId())
                .bind("routeId", event.routeId())
                .bind("vanId", event.vanId())
                .bind("stopIndex", event.stopIndex())
                .bind("customerId", event.customerId())
                .bind("lon", event.location().longitude())
                .bind("lat", event.location().latitude())
                .bind("slaDeadline", event.slaDeadline() != null ? event.slaDeadline() : event.timestamp())
                .bind("arrival", event.timestamp())
                .bind("parcels", event.parcelsDelivered())
                .bind("status", deliveryStatus)
                .bind("breached", breached)
                .bind("breachSeconds", (int) Math.min(Integer.MAX_VALUE, breachSeconds))
                .then();
    }

    /** @return rows inserted: 0 when this event's breach was already recorded */
    private Mono<Long> insertSlaBreach(DeliveryEvent event, long breachSeconds, String severity,
            UUID geofenceId, Instant predictedArrival) {
        DatabaseClient.GenericExecuteSpec spec = db.sql("""
                INSERT INTO sla_breaches (event_id, route_id, van_id, stop_index, customer_id,
                    sla_deadline, predicted_arrival, actual_arrival,
                    breach_seconds, severity, cause, geofence_id)
                VALUES (:eventId, :routeId, :vanId, :stopIndex, :customerId,
                    :slaDeadline, :predicted, :arrival,
                    :breachSeconds, :severity, 'LATE_ARRIVAL', :geofenceId)
                ON CONFLICT (event_id) DO NOTHING
                """)
                .bind("eventId", event.eventId())
                .bind("routeId", event.routeId())
                .bind("vanId", event.vanId())
                .bind("stopIndex", event.stopIndex())
                .bind("customerId", event.customerId())
                .bind("slaDeadline", event.slaDeadline())
                .bind("arrival", event.timestamp())
                .bind("breachSeconds", (int) Math.min(Integer.MAX_VALUE, breachSeconds))
                .bind("severity", severity);
        spec = predictedArrival != null ? spec.bind("predicted", predictedArrival) : spec.bindNull("predicted", Instant.class);
        spec = geofenceId != null ? spec.bind("geofenceId", geofenceId) : spec.bindNull("geofenceId", UUID.class);
        return spec.fetch().rowsUpdated();
    }

    private static String rootMessage(Throwable e) {
        Throwable t = e;
        while (t.getCause() != null && t.getCause() != t) {
            t = t.getCause();
        }
        return t.getClass().getSimpleName() + ": " + t.getMessage();
    }
}
