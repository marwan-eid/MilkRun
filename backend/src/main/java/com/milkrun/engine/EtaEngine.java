package com.milkrun.engine;

import com.milkrun.model.GpsEvent;
import com.milkrun.model.Location;
import com.milkrun.model.RoutePlan;
import com.milkrun.model.VanState;
import com.milkrun.model.VanState.DataConfidence;
import com.milkrun.model.VanState.EtaMethod;
import com.milkrun.model.VanState.SlaRisk;
import com.milkrun.model.VanStatus;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Predicts when each van reaches its next stop and whether it will make the
 * stop's delivery slot.
 *
 * The primary estimate walks the planned route from the van's matched position
 * to the stop, slowing each segment by its delay-zone factor, at the van's
 * free-flow speed. That speed is learned from how fast the van actually
 * progresses along the route (normalised by the zone it was in), starting from
 * its reported speed. SLA risk compares the predicted arrival with the stop's
 * deadline.
 *
 * The estimator runs behind a circuit breaker. If it keeps throwing (for
 * example on a malformed plan), or the van is off its planned route, the
 * straight-line distance times a detour factor is used instead.
 */
@Service
public class EtaEngine {

    private static final Logger log = LoggerFactory.getLogger(EtaEngine.class);
    private static final double EMA_ALPHA = 0.3;

    private final CircuitBreaker circuitBreaker;
    private final GeofenceDetector geofence;
    private final RoutePlanStore planStore;
    private final long warningBufferSeconds;
    private final long criticalBufferSeconds;
    private final double detourFactor;
    private final double offRouteMeters;

    private final ConcurrentHashMap<String, VanState> vanStates = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Track> tracks = new ConcurrentHashMap<>();

    private final Counter etaCalculations;
    private final Counter circuitBreakerFallbacks;
    private final Counter offRoute;
    private final DistributionSummary etaError;

    public EtaEngine(
            CircuitBreaker circuitBreaker,
            GeofenceDetector geofence,
            RoutePlanStore planStore,
            MeterRegistry meterRegistry,
            @Value("${milkrun.eta.sla-warning-buffer-seconds:60}") long warningBufferSeconds,
            @Value("${milkrun.eta.sla-critical-buffer-seconds:0}") long criticalBufferSeconds,
            @Value("${milkrun.eta.detour-factor:1.3}") double detourFactor,
            @Value("${milkrun.eta.off-route-meters:200}") double offRouteMeters) {
        this.circuitBreaker = circuitBreaker;
        this.geofence = geofence;
        this.planStore = planStore;
        this.warningBufferSeconds = warningBufferSeconds;
        this.criticalBufferSeconds = criticalBufferSeconds;
        this.detourFactor = detourFactor;
        this.offRouteMeters = offRouteMeters;

        this.etaCalculations = Counter.builder("milkrun.eta.calculations")
                .description("ETA calculations performed").register(meterRegistry);
        this.circuitBreakerFallbacks = Counter.builder("milkrun.circuit_breaker.fallbacks")
                .description("ETAs computed by the straight-line fallback because the route estimator failed or its breaker was open")
                .register(meterRegistry);
        this.offRoute = Counter.builder("milkrun.eta.off_route")
                .description("ETAs computed by the straight-line fallback because the van was off its planned route")
                .register(meterRegistry);
        this.etaError = DistributionSummary.builder("milkrun.eta.error.seconds")
                .description("Absolute error of the ETA predicted when the van set off for a stop, measured on arrival")
                .baseUnit("seconds")
                .publishPercentiles(0.5, 0.9)
                .publishPercentileHistogram()
                .register(meterRegistry);
        Gauge.builder("milkrun.vans.tracked", vanStates, Map::size)
                .description("Vans with a known state").register(meterRegistry);
        for (SlaRisk risk : SlaRisk.values()) {
            Gauge.builder("milkrun.vans.sla_risk", this, e -> e.countByRisk(risk))
                    .description("Vans whose next stop currently has this SLA risk")
                    .tag("level", risk.name())
                    .register(meterRegistry);
        }
    }

    /** Per-van state carried between GPS events. Guarded by its own monitor. */
    private static final class Track {
        String routeId;
        VanStatus lastStatus;
        int lastStopIndex = -1;
        double lastDistance = Double.NaN;
        Instant lastTimestamp;
        /** Learned free-flow speed in wall-clock metres per second; NaN until measured. */
        double freeFlowMps = Double.NaN;
        /** Predicted arrival made when the van set off for each stop, by customer id. */
        final Map<String, Instant> firstPrediction = new HashMap<>();
        /** Actual arrival at each stop, by customer id. */
        final Map<String, Instant> arrivals = new HashMap<>();

        void resetIfNewRoute(String routeId) {
            if (!routeId.equals(this.routeId)) {
                this.routeId = routeId;
                lastStatus = null;
                lastStopIndex = -1;
                lastDistance = Double.NaN;
                lastTimestamp = null;
                firstPrediction.clear();
                arrivals.clear();
                // freeFlowMps is kept: it describes the van, not the route
            }
        }
    }

    private record Estimate(long etaSeconds, EtaMethod method, Instant deadline, Instant arrival, SlaRisk risk) {
        static final Estimate UNKNOWN = new Estimate(-1, EtaMethod.NONE, null, null, SlaRisk.UNKNOWN);
        static final Estimate NO_NEXT_STOP = new Estimate(-1, EtaMethod.NONE, null, null, SlaRisk.NONE);

        Long slackSeconds() {
            return deadline == null || arrival == null ? null : Duration.between(arrival, deadline).toSeconds();
        }
    }

    public VanState processGpsEvent(GpsEvent event) {
        etaCalculations.increment();
        GeofenceDetector.GeofenceResult zone = geofence.check(event.location());

        Track track = tracks.computeIfAbsent(event.vanId(), k -> new Track());
        Estimate estimate;
        synchronized (track) {
            track.resetIfNewRoute(event.routeId());
            estimate = planStore.get(event.vanId(), event.routeId())
                    .map(planned -> estimate(event, planned, track))
                    .orElse(Estimate.UNKNOWN);
        }

        VanState state = new VanState(
                event.vanId(),
                event.routeId(),
                event.location(),
                event.speedKmh(),
                event.headingDegrees(),
                event.batteryPct(),
                event.status(),
                event.currentStopIndex(),
                event.totalStops(),
                estimate.etaSeconds(),
                estimate.risk(),
                assessConfidence(event),
                zone.inGeofence(),
                zone.zoneName(),
                Instant.now(),
                estimate.deadline(),
                estimate.arrival(),
                estimate.slackSeconds(),
                estimate.method());
        vanStates.put(event.vanId(), state);
        return state;
    }

    private Estimate estimate(GpsEvent event, RoutePlanStore.PlannedRoute planned, Track track) {
        List<RoutePlan.Stop> stops = planned.plan().stops();
        int k = event.currentStopIndex();
        if (k < 0 || k >= stops.size()) {
            return Estimate.NO_NEXT_STOP;
        }
        RoutePlan.Stop stop = stops.get(k);

        if (event.status() == VanStatus.DELIVERING) {
            Instant arrival = track.arrivals.computeIfAbsent(stop.customerId(),
                    c -> recordArrival(track, c, event.deviceTimestamp()));
            track.lastStatus = VanStatus.DELIVERING;
            track.lastStopIndex = k;
            track.lastDistance = planned.geometry().distanceAt(stop.waypointIndex());
            track.lastTimestamp = event.deviceTimestamp();
            SlaRisk risk = arrival.isAfter(stop.slaDeadline()) ? SlaRisk.CRITICAL : SlaRisk.NONE;
            return new Estimate(0, EtaMethod.ROUTE, stop.slaDeadline(), arrival, risk);
        }
        if (event.status() != VanStatus.EN_ROUTE) {
            return Estimate.NO_NEXT_STOP;
        }

        Estimate estimate = null;
        try {
            estimate = circuitBreaker.executeSupplier(() -> routeEstimate(event, planned, track, k));
        } catch (Exception e) {
            circuitBreakerFallbacks.increment();
            log.debug("Route estimator failed for van={}: {}", event.vanId(), e.toString());
        }
        if (estimate == null) {
            estimate = straightLineEstimate(event, planned.plan(), stop, track);
        }
        track.firstPrediction.putIfAbsent(stop.customerId(), estimate.arrival());
        return estimate;
    }

    /**
     * Along-route estimate, or null when the van is too far from the planned
     * leg for the route to be trusted.
     */
    private Estimate routeEstimate(GpsEvent event, RoutePlanStore.PlannedRoute planned, Track track, int k) {
        RoutePlan plan = planned.plan();
        RouteGeometry geometry = planned.geometry();
        RoutePlan.Stop stop = plan.stops().get(k);
        int fromVertex = k == 0 ? 0 : plan.stops().get(k - 1).waypointIndex();

        RouteGeometry.Match match = geometry.locate(
                event.location().latitude(), event.location().longitude(), fromVertex, stop.waypointIndex());
        if (match.offRouteMeters() > offRouteMeters) {
            offRoute.increment();
            return null;
        }
        learnSpeed(track, event, k, match.distanceAlong(), geometry);

        double freeFlow = !Double.isNaN(track.freeFlowMps)
                ? track.freeFlowMps
                : priorFreeFlow(event, plan, geometry.factorAt(match.distanceAlong()));
        double seconds = geometry.travelSeconds(match.distanceAlong(), stop.waypointIndex(), freeFlow);
        return estimateFor(stop, event.deviceTimestamp(), seconds, EtaMethod.ROUTE);
    }

    private Estimate straightLineEstimate(GpsEvent event, RoutePlan plan, RoutePlan.Stop stop, Track track) {
        Location p = event.location();
        double meters = RouteGeometry.haversineMeters(p.latitude(), p.longitude(),
                stop.location().latitude(), stop.location().longitude()) * detourFactor;
        double factor = geofence.speedFactorAt(p.latitude(), p.longitude());
        double speed = !Double.isNaN(track.freeFlowMps)
                ? track.freeFlowMps * factor
                : priorFreeFlow(event, plan, factor) * factor;
        return estimateFor(stop, event.deviceTimestamp(), speed > 0 ? meters / speed : 0, EtaMethod.STRAIGHT_LINE);
    }

    private Estimate estimateFor(RoutePlan.Stop stop, Instant from, double seconds, EtaMethod method) {
        long eta = Math.round(seconds);
        Instant arrival = from.plusSeconds(eta);
        long slack = Duration.between(arrival, stop.slaDeadline()).toSeconds();
        SlaRisk risk = slack < criticalBufferSeconds ? SlaRisk.CRITICAL
                : slack < warningBufferSeconds ? SlaRisk.WARNING
                : SlaRisk.NONE;
        return new Estimate(eta, method, stop.slaDeadline(), arrival, risk);
    }

    /**
     * Updates the van's free-flow speed from its progress along the route since
     * the previous event of the same leg.
     */
    private void learnSpeed(Track track, GpsEvent event, int stopIndex, double distance, RouteGeometry geometry) {
        if (track.lastStatus == VanStatus.EN_ROUTE && track.lastStopIndex == stopIndex
                && !Double.isNaN(track.lastDistance) && track.lastTimestamp != null) {
            double dt = Duration.between(track.lastTimestamp, event.deviceTimestamp()).toMillis() / 1000.0;
            double ds = distance - track.lastDistance;
            if (dt >= 0.2 && dt <= 30 && ds >= 0) {
                double factor = geometry.factorAt((distance + track.lastDistance) / 2);
                double sample = (ds / dt) / factor;
                track.freeFlowMps = Double.isNaN(track.freeFlowMps)
                        ? sample
                        : EMA_ALPHA * sample + (1 - EMA_ALPHA) * track.freeFlowMps;
            }
        }
        track.lastStatus = VanStatus.EN_ROUTE;
        track.lastStopIndex = stopIndex;
        track.lastDistance = distance;
        track.lastTimestamp = event.deviceTimestamp();
    }

    /**
     * Free-flow speed in wall-clock m/s before anything has been measured: the
     * reported (simulated) speed scaled by the time scale and un-slowed by the
     * zone, or the plan's base speed.
     */
    private static double priorFreeFlow(GpsEvent event, RoutePlan plan, double zoneFactor) {
        double simKmh = event.speedKmh() > 0 ? event.speedKmh() / zoneFactor : plan.baseSpeedKmh();
        return simKmh / 3.6 * plan.timeScale();
    }

    private Instant recordArrival(Track track, String customerId, Instant arrival) {
        Instant predicted = track.firstPrediction.get(customerId);
        if (predicted != null) {
            etaError.record(Math.abs(Duration.between(predicted, arrival).toMillis()) / 1000.0);
        }
        return arrival;
    }

    /**
     * The arrival predicted when the van set off for this stop, if this
     * instance saw it.
     */
    public Instant initialPrediction(String vanId, String routeId, String customerId) {
        Track track = tracks.get(vanId);
        if (track == null) {
            return null;
        }
        synchronized (track) {
            return routeId.equals(track.routeId) ? track.firstPrediction.get(customerId) : null;
        }
    }

    /** The van's learned free-flow speed in wall-clock m/s, if it has been measured. */
    public OptionalDouble freeFlowSpeed(String vanId) {
        Track track = tracks.get(vanId);
        if (track == null) {
            return OptionalDouble.empty();
        }
        synchronized (track) {
            return Double.isNaN(track.freeFlowMps) ? OptionalDouble.empty() : OptionalDouble.of(track.freeFlowMps);
        }
    }

    private DataConfidence assessConfidence(GpsEvent event) {
        if (event.ingestionTimestamp() == null) {
            return DataConfidence.STALE;
        }
        long lagMs = Duration.between(event.deviceTimestamp(), event.ingestionTimestamp()).toMillis();
        if (lagMs < 2000) return DataConfidence.REAL_TIME;
        if (lagMs < 10000) return DataConfidence.INTERPOLATED;
        return DataConfidence.STALE;
    }

    private long countByRisk(SlaRisk risk) {
        return vanStates.values().stream().filter(v -> v.slaRisk() == risk).count();
    }

    /** Latest state of every van. */
    public ConcurrentHashMap<String, VanState> getAllVanStates() {
        return vanStates;
    }

    /** Haversine distance between two locations in km. */
    public static double haversineDistance(Location a, Location b) {
        return RouteGeometry.haversineMeters(a.latitude(), a.longitude(), b.latitude(), b.longitude()) / 1000;
    }
}
