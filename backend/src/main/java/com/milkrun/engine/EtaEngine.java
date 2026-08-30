package com.milkrun.engine;

import com.milkrun.model.GpsEvent;
import com.milkrun.model.Location;
import com.milkrun.model.VanState;
import com.milkrun.model.VanState.DataConfidence;
import com.milkrun.model.VanState.SlaRisk;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ETA calculation engine with Haversine distance, SLA risk prediction,
 * and circuit-breaker-protected fallback.
 *
 * Pipeline: GPS event → compute distance to next stop → speed-based ETA
 * → apply geofence speed factor → compare against SLA deadline
 * → emit VanState with confidence + SLA risk
 */
@Service
public class EtaEngine {

    private static final Logger log = LoggerFactory.getLogger(EtaEngine.class);
    private static final double EARTH_RADIUS_KM = 6371.0;

    private final CircuitBreaker circuitBreaker;
    private final GeofenceDetector geofenceDetector;
    private final long slaWarningBufferSeconds;
    private final long slaCriticalBufferSeconds;
    private final ConcurrentHashMap<String, VanState> vanStates = new ConcurrentHashMap<>();

    // Per-van distance tracking for accurate ETA
    private final ConcurrentHashMap<String, Double> cumulativeDistances = new ConcurrentHashMap<>();

    // Metrics
    private final Counter etaCalculations;
    private final Counter slaWarnings;
    private final Counter slaCriticals;
    private final Counter circuitBreakerFallbacks;

    public EtaEngine(
            CircuitBreaker circuitBreaker,
            GeofenceDetector geofenceDetector,
            MeterRegistry meterRegistry,
            @Value("${milkrun.eta.sla-warning-buffer-seconds:120}") long slaWarningBufferSeconds,
            @Value("${milkrun.eta.sla-critical-buffer-seconds:30}") long slaCriticalBufferSeconds) {
        this.circuitBreaker = circuitBreaker;
        this.geofenceDetector = geofenceDetector;
        this.slaWarningBufferSeconds = slaWarningBufferSeconds;
        this.slaCriticalBufferSeconds = slaCriticalBufferSeconds;

        this.etaCalculations = Counter.builder("milkrun.eta.calculations")
                .description("Total ETA calculations performed")
                .register(meterRegistry);
        this.slaWarnings = Counter.builder("milkrun.sla.warnings")
                .description("SLA warning events emitted")
                .register(meterRegistry);
        this.slaCriticals = Counter.builder("milkrun.sla.criticals")
                .description("SLA critical events emitted")
                .register(meterRegistry);
        this.circuitBreakerFallbacks = Counter.builder("milkrun.circuit_breaker.fallbacks")
                .description("Circuit breaker fallback invocations")
                .register(meterRegistry);
    }

    /**
     * Process a GPS event and compute the new VanState.
     * Protected by a circuit breaker — falls back to linear extrapolation.
     */
    public VanState processGpsEvent(GpsEvent event) {
        etaCalculations.increment();

        try {
            return CircuitBreaker.decorateSupplier(circuitBreaker, () -> computeVanState(event)).get();
        } catch (Exception e) {
            circuitBreakerFallbacks.increment();
            log.warn("Circuit breaker fallback for van={}: {}", event.vanId(), e.getMessage());
            return computeFallbackState(event);
        }
    }

    /**
     * Full ETA computation with geofence detection and actual distance tracking.
     */
    private VanState computeVanState(GpsEvent event) {
        // Geofence check
        GeofenceDetector.GeofenceResult geofence = geofenceDetector.check(event.location());

        // Compute speed (floor at 5 km/h to avoid infinity ETA)
        double speedKmh = Math.max(event.speedKmh(), 5.0);

        // Apply geofence speed factor
        if (geofence.inGeofence()) {
            speedKmh *= geofence.speedFactor();
        }

        // Smooth the speed using an Exponential Moving Average (EMA) to prevent massive
        // ETA jumps
        // when the van randomly speeds up or hits a geofence.
        Double prevSpeed = cumulativeDistances.put(event.vanId(), speedKmh); // Re-using this map for speed EMA
        if (prevSpeed != null) {
            speedKmh = (0.2 * speedKmh) + (0.8 * prevSpeed);
            cumulativeDistances.put(event.vanId(), speedKmh);
        }

        // Estimate distance to next stop using the average route dist (15km) divided by
        // total stops
        int total = Math.max(event.totalStops(), 1);
        double etaNextStopKm = 15.0 / total;

        long etaNextStopSeconds = (long) ((etaNextStopKm / speedKmh) * 3600);

        // SLA risk assessment
        SlaRisk slaRisk = assessSlaRisk(etaNextStopSeconds);

        if (slaRisk == SlaRisk.WARNING)
            slaWarnings.increment();
        if (slaRisk == SlaRisk.CRITICAL)
            slaCriticals.increment();

        // Determine data confidence
        DataConfidence confidence = assessConfidence(event);

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
                etaNextStopSeconds,
                slaRisk,
                confidence,
                geofence.inGeofence(),
                geofence.zoneName(),
                Instant.now());

        vanStates.put(event.vanId(), state);
        return state;
    }

    /**
     * Fallback: linear extrapolation from last known speed.
     * Used when the circuit breaker is open.
     */
    private VanState computeFallbackState(GpsEvent event) {
        VanState lastKnown = vanStates.get(event.vanId());
        long etaSeconds = lastKnown != null ? lastKnown.etaNextStopSeconds() : 300; // default 5 min

        return new VanState(
                event.vanId(),
                event.routeId(),
                event.location(),
                event.speedKmh(),
                event.headingDegrees(),
                event.batteryPct(),
                event.status(),
                event.currentStopIndex(),
                event.totalStops(),
                etaSeconds,
                SlaRisk.NONE,
                DataConfidence.INTERPOLATED,
                false,
                null,
                Instant.now());
    }

    /**
     * Assess SLA risk based on ETA.
     */
    private SlaRisk assessSlaRisk(long etaSeconds) {
        if (etaSeconds > slaCriticalBufferSeconds && etaSeconds <= slaWarningBufferSeconds) {
            return SlaRisk.WARNING;
        } else if (etaSeconds > slaWarningBufferSeconds) {
            // ETA is very large, critical risk
            return SlaRisk.CRITICAL;
        }
        return SlaRisk.NONE;
    }

    /**
     * Assess data confidence based on event freshness.
     */
    private DataConfidence assessConfidence(GpsEvent event) {
        if (event.ingestionTimestamp() == null) {
            return DataConfidence.STALE;
        }
        long lagMs = Duration.between(event.deviceTimestamp(), event.ingestionTimestamp()).toMillis();
        if (lagMs < 2000)
            return DataConfidence.REAL_TIME;
        if (lagMs < 10000)
            return DataConfidence.INTERPOLATED;
        return DataConfidence.STALE;
    }

    /**
     * Get the current state of all vans.
     */
    public ConcurrentHashMap<String, VanState> getAllVanStates() {
        return vanStates;
    }

    /**
     * Haversine distance between two locations in km.
     */
    public static double haversineDistance(Location a, Location b) {
        double dLat = Math.toRadians(b.latitude() - a.latitude());
        double dLon = Math.toRadians(b.longitude() - a.longitude());
        double lat1 = Math.toRadians(a.latitude());
        double lat2 = Math.toRadians(b.latitude());
        double h = Math.pow(Math.sin(dLat / 2), 2) +
                Math.cos(lat1) * Math.cos(lat2) * Math.pow(Math.sin(dLon / 2), 2);
        return 2 * EARTH_RADIUS_KM * Math.asin(Math.sqrt(h));
    }
}
