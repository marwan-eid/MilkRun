package com.milkrun.observability;

import com.milkrun.fleet.FleetView;
import com.milkrun.model.VanState;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.io.File;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.DoubleSupplier;
import java.util.function.Supplier;

/**
 * Rolling view of the GPS pipeline over the last few minutes.
 *
 * Lifetime counters hide problems: after days of healthy traffic, a pipeline
 * that suddenly rejects 99% of events still shows a modest lifetime rejection
 * rate. This samples the Micrometer counters periodically and reports rates
 * over a sliding window, plus a status that external checks can alert on.
 * The status also covers what the counters cannot show: vans that went silent
 * while others still report, and a disk that is filling up.
 */
@Component
public class PipelineHealthMonitor {

    private static final Logger log = LoggerFactory.getLogger(PipelineHealthMonitor.class);

    static final String RECEIVED = "milkrun.events.received";
    static final String DEDUPLICATED = "milkrun.events.deduplicated";
    static final String PROCESSED = "milkrun.events.processed";

    public enum Status { WARMING_UP, OK, DEGRADED }

    private final MeterRegistry registry;
    private final Clock clock;
    private final Duration window;
    private final Duration sampleInterval;
    private final double maxRejectionRate;
    private final DoubleSupplier diskFree;
    private final double minDiskFree;
    private final Supplier<Collection<Instant>> vanLastSeen;
    private final double maxSilentVans;
    private final Deque<Sample> samples = new ArrayDeque<>();
    private Disposable sampler;

    @Autowired
    public PipelineHealthMonitor(
            MeterRegistry registry,
            @Value("${milkrun.health.window:5m}") Duration window,
            @Value("${milkrun.health.sample-interval:15s}") Duration sampleInterval,
            @Value("${milkrun.health.max-dedup-rejection-rate:0.20}") double maxRejectionRate,
            @Value("${milkrun.health.min-disk-free:0.15}") double minDiskFree,
            @Value("${milkrun.health.max-silent-vans:0.05}") double maxSilentVans,
            FleetView fleet) {
        this(registry, Clock.systemUTC(), window, sampleInterval, maxRejectionRate,
                PipelineHealthMonitor::diskFreeFraction, minDiskFree,
                () -> fleet.all().stream().map(VanState::lastUpdated).toList(), maxSilentVans);
    }

    PipelineHealthMonitor(MeterRegistry registry, Clock clock, Duration window,
            Duration sampleInterval, double maxRejectionRate) {
        this(registry, clock, window, sampleInterval, maxRejectionRate, () -> 1.0, 0, List::of, 1);
    }

    /**
     * @param diskFree      free share of the disk, 0 to 1
     * @param vanLastSeen   when each known van last reported
     * @param maxSilentVans share of the fleet that may be silent for a whole window
     */
    PipelineHealthMonitor(MeterRegistry registry, Clock clock, Duration window,
            Duration sampleInterval, double maxRejectionRate, DoubleSupplier diskFree, double minDiskFree,
            Supplier<Collection<Instant>> vanLastSeen, double maxSilentVans) {
        this.registry = registry;
        this.clock = clock;
        this.window = window;
        this.sampleInterval = sampleInterval;
        this.maxRejectionRate = maxRejectionRate;
        this.diskFree = diskFree;
        this.minDiskFree = minDiskFree;
        this.vanLastSeen = vanLastSeen;
        this.maxSilentVans = maxSilentVans;
    }

    /**
     * Free share of the disk the container's filesystem lives on, which on a
     * single-VM deployment is the disk Kafka and PostgreSQL write to as well.
     */
    private static double diskFreeFraction() {
        File root = new File("/");
        long total = root.getTotalSpace();
        return total > 0 ? (double) root.getUsableSpace() / total : 1.0;
    }

    @PostConstruct
    void start() {
        sample();
        sampler = Flux.interval(sampleInterval)
                .subscribe(tick -> sample(), e -> log.error("Health sampler stopped", e));
    }

    @PreDestroy
    void stop() {
        if (sampler != null) {
            sampler.dispose();
        }
    }

    /** Records the current counter values and drops samples older than the window. */
    synchronized void sample() {
        Instant now = clock.instant();
        samples.addLast(new Sample(now, count(RECEIVED), count(DEDUPLICATED), count(PROCESSED)));
        // Keep one sample at or beyond the window edge so deltas span the full window.
        while (samples.size() > 2) {
            Iterator<Sample> it = samples.iterator();
            it.next();
            if (it.next().at().isAfter(now.minus(window))) {
                break;
            }
            samples.removeFirst();
        }
    }

    public synchronized Report report() {
        Instant now = clock.instant();
        if (samples.size() < 2) {
            return new Report(Status.WARMING_UP, 0, 0, 0, 0, 0, 0, List.of());
        }
        Sample oldest = samples.peekFirst();
        Sample newest = samples.peekLast();
        double seconds = Math.max(1, Duration.between(oldest.at(), newest.at()).toMillis() / 1000.0);
        long received = Math.round(newest.received() - oldest.received());
        long rejected = Math.round(newest.deduplicated() - oldest.deduplicated());
        long processed = Math.round(newest.processed() - oldest.processed());
        double rejectionRate = received > 0 ? (double) rejected / received : 0;

        List<String> problems = new ArrayList<>();
        boolean fullWindow = !oldest.at().isAfter(now.minus(window).plus(sampleInterval));
        if (received > 0 && rejectionRate > maxRejectionRate) {
            problems.add(String.format("dedup rejected %.1f%% of events (limit %.0f%%)",
                    rejectionRate * 100, maxRejectionRate * 100));
        }
        if (received > 0 && processed == 0) {
            problems.add("events are arriving but none were processed");
        }
        if (received == 0 && fullWindow) {
            problems.add("no GPS events received in the last " + window.toMinutes() + " min");
        }
        double free = diskFree.getAsDouble();
        if (free < minDiskFree) {
            problems.add(String.format("disk is %.0f%% full (limit %.0f%%)", (1 - free) * 100, (1 - minDiskFree) * 100));
        }
        // Part of the fleet can stop reporting while the rest keeps the counters
        // moving (a Kafka partition that stops taking writes silences its vans).
        // No healthy van stays quiet for a whole window: dead zones last seconds,
        // and a van that finished its route starts a new one right away.
        Collection<Instant> lastSeen = vanLastSeen.get();
        Instant silentBefore = now.minus(window);
        long silent = lastSeen.stream().filter(seen -> seen != null && seen.isBefore(silentBefore)).count();
        if (silent > lastSeen.size() * maxSilentVans) {
            problems.add(String.format("%d of %d vans have not reported in the last %d min",
                    silent, lastSeen.size(), window.toMinutes()));
        }

        Status status = !problems.isEmpty() ? Status.DEGRADED : fullWindow ? Status.OK : Status.WARMING_UP;
        return new Report(status, (long) seconds, received, rejected, processed,
                round1(rejectionRate * 100), round1(processed / seconds), problems);
    }

    private double count(String name) {
        Counter counter = registry.find(name).counter();
        return counter != null ? counter.count() : 0;
    }

    private static double round1(double v) {
        return Math.round(v * 10) / 10.0;
    }

    private record Sample(Instant at, double received, double deduplicated, double processed) {
    }

    public record Report(Status status, long windowSeconds, long received, long rejected, long processed,
            double rejectionRatePct, double processedPerSecond, List<String> problems) {

        public Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("status", status.name());
            m.put("window_seconds", windowSeconds);
            m.put("events_received", received);
            m.put("dedup_rejected", rejected);
            m.put("dedup_rejection_rate_pct", rejectionRatePct);
            m.put("events_processed", processed);
            m.put("processed_per_sec", processedPerSecond);
            m.put("problems", problems);
            return m;
        }
    }
}
