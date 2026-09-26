package com.milkrun.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.milkrun.model.RoutePlan;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Latest route plan of every van, read from the compacted route-plans topic.
 *
 * Every backend instance needs every plan (dispatch and analytics look at the
 * whole fleet), so each instance reads the topic from the beginning with its
 * own throwaway consumer group and never commits offsets. Compaction keeps
 * that replay to roughly one record per van.
 */
@Component
public class RoutePlanStore {

    private static final Logger log = LoggerFactory.getLogger(RoutePlanStore.class);

    /** A plan with its geometry prepared for ETA calculations. */
    public record PlannedRoute(RoutePlan plan, RouteGeometry geometry) {
    }

    private final ObjectMapper objectMapper;
    private final GeofenceDetector geofence;
    private final DatabaseClient db;
    private final String bootstrapServers;
    private final String topic;
    private final ConcurrentHashMap<String, PlannedRoute> byVan = new ConcurrentHashMap<>();
    private final Counter plansReceived;
    private final Counter plansRejected;
    private Disposable subscription;

    public RoutePlanStore(ObjectMapper objectMapper, GeofenceDetector geofence, DatabaseClient db,
            MeterRegistry meterRegistry,
            @Value("${milkrun.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${milkrun.kafka.route-plans-topic}") String topic) {
        this.objectMapper = objectMapper;
        this.geofence = geofence;
        this.db = db;
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.plansReceived = Counter.builder("milkrun.route_plans.received")
                .description("Route plans received").register(meterRegistry);
        this.plansRejected = Counter.builder("milkrun.route_plans.rejected")
                .description("Route plans that were malformed or older than the one held").register(meterRegistry);
        Gauge.builder("milkrun.route_plans.held", byVan, Map::size)
                .description("Vans with a known route plan").register(meterRegistry);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "milkrun-route-plans-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        ReceiverOptions<String, String> options = ReceiverOptions.<String, String>create(props)
                .subscription(List.of(topic));

        subscription = KafkaReceiver.create(options).receive()
                // After a restart the replay delivers every plan each van ever had
                // (compaction lags behind), so records are handled in batches and
                // only each van's newest plan in a batch is prepared and recorded.
                .bufferTimeout(500, Duration.ofMillis(200), true)
                .concatMap(batch -> acceptAll(batch.stream().map(ConsumerRecord::value).toList()))
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1)).maxBackoff(Duration.ofSeconds(30))
                        .doBeforeRetry(s -> log.warn("Route plan consumer restarting: {}", s.failure().toString())))
                .subscribe();
        log.info("Route plan consumer started on {}", topic);
    }

    @PreDestroy
    void stop() {
        if (subscription != null) {
            subscription.dispose();
        }
    }

    /** Parses, stores and records one plan. Bad plans are counted and skipped. */
    Mono<Void> accept(String json) {
        return acceptAll(List.of(json));
    }

    /**
     * Same outcome as accepting the plans one by one, in order, but only the
     * plan each van ends up with is prepared (geometry) and recorded; plans a
     * later one in the batch supersedes are counted and skipped.
     */
    Mono<Void> acceptAll(List<String> batch) {
        Map<String, RoutePlan> winners = new LinkedHashMap<>();
        for (String json : batch) {
            RoutePlan plan = parse(json);
            if (plan == null) {
                continue;
            }
            if (plan.validationError() != null) {
                put(plan); // counts and logs the rejection
                continue;
            }
            RoutePlan current = winners.containsKey(plan.vanId()) ? winners.get(plan.vanId()) : held(plan.vanId());
            if (current == null || isNewer(plan, current)) {
                if (winners.put(plan.vanId(), plan) != null) {
                    plansReceived.increment(); // the plan it replaces
                }
            } else {
                plansReceived.increment();
                plansRejected.increment();
            }
        }
        return Flux.fromIterable(winners.values()).concatMap(this::store).then();
    }

    private RoutePlan parse(String json) {
        try {
            return objectMapper.readValue(json, RoutePlan.class);
        } catch (Exception e) {
            plansRejected.increment();
            log.warn("Unreadable route plan: {}", e.getMessage());
            return null;
        }
    }

    private RoutePlan held(String vanId) {
        PlannedRoute planned = byVan.get(vanId);
        return planned != null ? planned.plan() : null;
    }

    /** Stores the plan and records its planned figures. */
    private Mono<Void> store(RoutePlan plan) {
        PlannedRoute stored = put(plan);
        if (stored == null) {
            return Mono.empty();
        }
        return recordPlannedRoute(stored)
                .onErrorResume(e -> {
                    log.warn("Could not record plan of {}: {}", plan.routeId(), e.getMessage());
                    return Mono.empty();
                });
    }

    /**
     * Keeps the plan if it is newer than the one held for the van: a later
     * version of the same route, or a different route created no earlier.
     *
     * @return the stored route, or null if the plan was rejected
     */
    public PlannedRoute put(RoutePlan plan) {
        plansReceived.increment();
        String error = plan.validationError();
        if (error != null) {
            plansRejected.increment();
            log.warn("Ignoring route plan {}: {}", plan.routeId(), error);
            return null;
        }
        PlannedRoute[] stored = { null };
        byVan.compute(plan.vanId(), (van, current) -> {
            if (current == null || isNewer(plan, current.plan())) {
                stored[0] = new PlannedRoute(plan, RouteGeometry.of(plan.waypoints(), geofence));
                return stored[0];
            }
            return current;
        });
        if (stored[0] == null) {
            plansRejected.increment();
        }
        return stored[0];
    }

    private static boolean isNewer(RoutePlan candidate, RoutePlan current) {
        if (candidate.routeId().equals(current.routeId())) {
            return candidate.version() > current.version();
        }
        Instant a = candidate.createdAt();
        Instant b = current.createdAt();
        return a == null || b == null || !a.isBefore(b);
    }

    /**
     * The van's current plan, if it is for {@code routeId}. Geometry is rebuilt
     * when the delay zones have changed since it was computed.
     */
    public Optional<PlannedRoute> get(String vanId, String routeId) {
        PlannedRoute planned = byVan.get(vanId);
        if (planned == null || !planned.plan().routeId().equals(routeId)) {
            return Optional.empty();
        }
        if (planned.geometry().zonesVersion() != geofence.version()) {
            PlannedRoute refreshed = new PlannedRoute(planned.plan(), RouteGeometry.of(planned.plan().waypoints(), geofence));
            byVan.replace(vanId, planned, refreshed);
            return Optional.of(refreshed);
        }
        return Optional.of(planned);
    }

    /** The latest plan held for every van. */
    public Map<String, PlannedRoute> all() {
        return Map.copyOf(byVan);
    }

    /**
     * Upserts the planned side of the route (hub to last delivery). Older plan
     * versions never overwrite newer ones, so replaying the topic is harmless.
     */
    private Mono<Void> recordPlannedRoute(PlannedRoute planned) {
        RoutePlan plan = planned.plan();
        RouteGeometry geometry = planned.geometry();
        if (db == null || plan.stops().isEmpty()) {
            return Mono.empty();
        }
        // A route is "completed" after its last delivery, so planned figures cover
        // the same span as the actual ones: hub to the last stop, plus its dwell.
        RoutePlan.Stop lastStop = plan.stops().get(plan.stops().size() - 1);
        double toLastStopMeters = geometry.distanceAt(lastStop.waypointIndex());
        Instant plannedEnd = lastStop.plannedArrival().plusMillis((long) (40 / plan.timeScale() * 1000));
        Instant plannedStart = plan.createdAt() != null ? plan.createdAt() : Instant.now();

        return db.sql("""
                INSERT INTO completed_routes (route_id, van_id, planned_start, planned_end, total_stops,
                                              plan_version, time_scale, planned_distance_km, status)
                VALUES (:routeId, :vanId, :plannedStart, :plannedEnd, :totalStops,
                        :version, :timeScale, :distanceKm, 'IN_PROGRESS')
                ON CONFLICT (route_id) DO UPDATE SET
                    planned_start = EXCLUDED.planned_start,
                    planned_end = EXCLUDED.planned_end,
                    total_stops = EXCLUDED.total_stops,
                    plan_version = EXCLUDED.plan_version,
                    time_scale = EXCLUDED.time_scale,
                    planned_distance_km = EXCLUDED.planned_distance_km
                WHERE completed_routes.plan_version IS NULL
                   OR completed_routes.plan_version < EXCLUDED.plan_version
                """)
                .bind("routeId", plan.routeId())
                .bind("vanId", plan.vanId())
                .bind("plannedStart", plannedStart)
                .bind("plannedEnd", plannedEnd)
                .bind("totalStops", plan.stops().size())
                .bind("version", plan.version())
                .bind("timeScale", plan.timeScale())
                .bind("distanceKm", Math.round(toLastStopMeters / 10.0) / 100.0)
                .then();
    }
}
