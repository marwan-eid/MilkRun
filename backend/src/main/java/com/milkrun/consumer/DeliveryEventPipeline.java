package com.milkrun.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.milkrun.engine.EtaEngine;
import com.milkrun.engine.GeofenceDetector;
import com.milkrun.model.DeliveryEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Persists delivery outcomes so the analytics layer can query them.
 *
 * Kafka (delivery-events) -> per event type:
 *   ARRIVAL            -> route row (if the plan has not created it yet), delivery log,
 *                         SLA breach if the van arrived after the slot ended
 *   DELIVERY_COMPLETED -> delivery log, route counters
 *   DELIVERY_FAILED    -> delivery log, route counters
 *   DEPARTURE          -> route marked COMPLETED after its last stop
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

    private final Counter deliveryEventsReceived;
    private final Counter deliveryEventsProcessed;
    private final Counter slaBreachesDetected;

    // In-memory route tracking: routeId -> RouteTracker
    private final ConcurrentHashMap<String, RouteTracker> routeTrackers = new ConcurrentHashMap<>();

    public DeliveryEventPipeline(
            @Qualifier("deliveryKafkaReceiver") KafkaReceiver<String, String> kafkaReceiver,
            ObjectMapper objectMapper,
            DatabaseClient databaseClient,
            MeterRegistry meterRegistry,
            GeofenceDetector geofenceDetector,
            EtaEngine etaEngine) {
        this.kafkaReceiver = kafkaReceiver;
        this.objectMapper = objectMapper;
        this.db = databaseClient;
        this.geofenceDetector = geofenceDetector;
        this.etaEngine = etaEngine;

        this.deliveryEventsReceived = Counter.builder("milkrun.delivery.events.received")
                .description("Delivery events received from Kafka")
                .register(meterRegistry);
        this.deliveryEventsProcessed = Counter.builder("milkrun.delivery.events.processed")
                .description("Delivery events persisted to DB")
                .register(meterRegistry);
        this.slaBreachesDetected = Counter.builder("milkrun.delivery.sla_breaches")
                .description("Stops where the van arrived after the delivery slot ended")
                .register(meterRegistry);
    }

    /** Starts once the application is ready, i.e. after Flyway has migrated the schema. */
    @EventListener(ApplicationReadyEvent.class)
    public void startPipeline() {
        kafkaReceiver.receive()
                .flatMap(record -> {
                    deliveryEventsReceived.increment();
                    try {
                        DeliveryEvent event = objectMapper.readValue(record.value(), DeliveryEvent.class);
                        record.receiverOffset().acknowledge();
                        return processDeliveryEvent(event).thenMany(Flux.empty());
                    } catch (Exception e) {
                        log.warn("Failed to deserialize delivery event: {}", e.getMessage());
                        record.receiverOffset().acknowledge();
                        return Flux.empty();
                    }
                })
                .doOnError(e -> log.error("Delivery pipeline error: {}", e.getMessage(), e))
                .retry()
                .subscribe();
        log.info("Delivery event pipeline started");
    }

    private Mono<Void> processDeliveryEvent(DeliveryEvent event) {
        return switch (event.eventType()) {
            case ARRIVAL -> handleArrival(event);
            case DELIVERY_COMPLETED -> handleCompletion(event, true);
            case DELIVERY_FAILED -> handleCompletion(event, false);
            case DEPARTURE -> handleDeparture(event);
        };
    }

    private Mono<Void> handleArrival(DeliveryEvent event) {
        RouteTracker tracker = routeTrackers.computeIfAbsent(event.routeId(),
                k -> new RouteTracker(event.vanId(), event.routeId()));
        if (tracker.startTime == null) {
            tracker.startTime = event.timestamp();
        }
        tracker.totalStops = event.totalStops();

        long breachSeconds = event.slaDeadline() != null && event.timestamp() != null
                ? Duration.between(event.slaDeadline(), event.timestamp()).toSeconds()
                : 0;
        boolean breached = breachSeconds > 0;

        Mono<Void> breach = Mono.empty();
        if (breached) {
            slaBreachesDetected.increment();
            GeofenceDetector.GeofenceResult zone = geofenceDetector.check(event.location());
            UUID zoneId = zone.inGeofence() ? UUID.fromString(zone.zoneId()) : null;
            Instant predicted = etaEngine.initialPrediction(event.vanId(), event.routeId(), event.customerId());
            String severity = breachSeconds > CRITICAL_BREACH_SECONDS ? "CRITICAL" : "WARNING";
            breach = insertSlaBreach(event, breachSeconds, severity, zoneId, predicted);
        }

        return ensureRoute(event)
                .then(insertDeliveryLog(event, "IN_PROGRESS", breached, Math.max(0, breachSeconds)))
                .then(breach)
                .doOnSuccess(v -> deliveryEventsProcessed.increment())
                .onErrorResume(e -> {
                    log.warn("Failed to persist ARRIVAL for van={}: {}", event.vanId(), e.getMessage());
                    return Mono.empty();
                });
    }

    private Mono<Void> handleCompletion(DeliveryEvent event, boolean success) {
        RouteTracker tracker = routeTrackers.computeIfAbsent(event.routeId(),
                k -> new RouteTracker(event.vanId(), event.routeId()));
        if (success) {
            tracker.completedStops++;
        } else {
            tracker.failedStops++;
        }

        return updateRouteCounters(tracker)
                .then(insertDeliveryLog(event, success ? "COMPLETED" : "FAILED", false, 0))
                .doOnSuccess(v -> deliveryEventsProcessed.increment())
                .onErrorResume(e -> {
                    log.warn("Failed to persist {} for van={}: {}", event.eventType(), event.vanId(), e.getMessage());
                    return Mono.empty();
                });
    }

    private Mono<Void> handleDeparture(DeliveryEvent event) {
        RouteTracker tracker = routeTrackers.get(event.routeId());
        if (tracker == null) {
            return Mono.empty();
        }
        Mono<Void> result = Mono.empty();
        if (tracker.completedStops + tracker.failedStops >= tracker.totalStops && tracker.totalStops > 0) {
            tracker.endTime = event.timestamp();
            result = completeRoute(tracker);
            routeTrackers.remove(event.routeId());
        }
        return result
                .doOnSuccess(v -> deliveryEventsProcessed.increment())
                .onErrorResume(e -> {
                    log.warn("Failed to persist DEPARTURE for van={}: {}", event.vanId(), e.getMessage());
                    return Mono.empty();
                });
    }

    // ═══════════════════ DB Operations ═══════════════════

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

    private Mono<Void> updateRouteCounters(RouteTracker tracker) {
        return db.sql("""
                UPDATE completed_routes
                SET completed_stops = :completed, failed_stops = :failed
                WHERE route_id = :routeId
                """)
                .bind("completed", tracker.completedStops)
                .bind("failed", tracker.failedStops)
                .bind("routeId", tracker.routeId)
                .then();
    }

    /**
     * Marks the route completed. Distance is the planned route length (until
     * the archived GPS path is used); speed is in simulated km/h, i.e. the
     * wall-clock duration scaled by the plan's time scale.
     */
    private Mono<Void> completeRoute(RouteTracker tracker) {
        Instant start = tracker.startTime != null ? tracker.startTime : Instant.now();
        Instant end = tracker.endTime != null ? tracker.endTime : Instant.now();
        double durationMin = Duration.between(start, end).toMillis() / 60_000.0;

        return db.sql("""
                UPDATE completed_routes
                SET status = 'COMPLETED',
                    actual_start = :actualStart,
                    actual_end = :actualEnd,
                    total_duration_min = :duration,
                    completed_stops = :completed,
                    failed_stops = :failed,
                    total_distance_km = planned_distance_km,
                    avg_speed_kmh = CASE
                        WHEN planned_distance_km IS NOT NULL AND time_scale > 0 AND :duration > 0
                        THEN LEAST(999.99, planned_distance_km / (:duration / 60.0 * time_scale))
                    END
                WHERE route_id = :routeId
                """)
                .bind("actualStart", start)
                .bind("actualEnd", end)
                .bind("duration", Math.round(durationMin * 100) / 100.0)
                .bind("completed", tracker.completedStops)
                .bind("failed", tracker.failedStops)
                .bind("routeId", tracker.routeId)
                .then()
                .doOnSuccess(v -> log.info("Route {} COMPLETED: {}/{} stops succeeded",
                        tracker.routeId, tracker.completedStops, tracker.totalStops));
    }

    private Mono<Void> insertDeliveryLog(DeliveryEvent event, String deliveryStatus, boolean breached, long breachSeconds) {
        return db.sql("""
                INSERT INTO delivery_logs (route_id, van_id, stop_index, customer_id, location,
                    sla_deadline, actual_arrival, parcels_delivered, delivery_status, sla_breached, breach_seconds)
                VALUES (:routeId, :vanId, :stopIndex, :customerId,
                    ST_SetSRID(ST_MakePoint(:lon, :lat), 4326),
                    :slaDeadline, :arrival, :parcels, :status, :breached, :breachSeconds)
                """)
                .bind("routeId", event.routeId())
                .bind("vanId", event.vanId())
                .bind("stopIndex", event.stopIndex())
                .bind("customerId", event.customerId())
                .bind("lon", event.location().longitude())
                .bind("lat", event.location().latitude())
                .bind("slaDeadline", event.slaDeadline() != null ? event.slaDeadline() : Instant.now())
                .bind("arrival", event.timestamp() != null ? event.timestamp() : Instant.now())
                .bind("parcels", event.parcelsDelivered())
                .bind("status", deliveryStatus)
                .bind("breached", breached)
                .bind("breachSeconds", (int) Math.min(Integer.MAX_VALUE, breachSeconds))
                .then();
    }

    private Mono<Void> insertSlaBreach(DeliveryEvent event, long breachSeconds, String severity,
            UUID geofenceId, Instant predictedArrival) {
        DatabaseClient.GenericExecuteSpec spec = db.sql("""
                INSERT INTO sla_breaches (route_id, van_id, stop_index, customer_id,
                    sla_deadline, predicted_arrival, actual_arrival,
                    breach_seconds, severity, cause, geofence_id)
                VALUES (:routeId, :vanId, :stopIndex, :customerId,
                    :slaDeadline, :predicted, :arrival,
                    :breachSeconds, :severity, 'LATE_ARRIVAL', :geofenceId)
                """)
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
        return spec.then();
    }

    // ═══════════════════ Route Tracker ═══════════════════

    private static class RouteTracker {
        final String vanId;
        final String routeId;
        Instant startTime;
        Instant endTime;
        int totalStops;
        int completedStops;
        int failedStops;

        RouteTracker(String vanId, String routeId) {
            this.vanId = vanId;
            this.routeId = routeId;
        }
    }
}
