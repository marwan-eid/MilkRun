package com.milkrun.persistence;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

/**
 * Deletes old rows so the database stays a bounded size. Without it the GPS
 * archive alone grew by about 0.5 GB a day and filled the demo VM's disk, and
 * the analytics queries, which aggregate whole tables, slowed past their timeout.
 *
 * Rows are deleted oldest first in small batches, a bounded number per run, so
 * a backlog is worked off over several runs instead of in one long transaction
 * that would stall the live pipeline's writes. Deleted space is reused by
 * PostgreSQL for new rows, so the files stop growing.
 */
@Component
@ConditionalOnProperty(name = "milkrun.retention.enabled", havingValue = "true", matchIfMissing = true)
public class RetentionJob {

    private static final Logger log = LoggerFactory.getLogger(RetentionJob.class);

    /*
     * gps_archive and dead_letter_log have no index on created_at, but their ids
     * come from a sequence, so the lowest ids are the oldest rows: take the
     * oldest batch through the primary key and delete those past the cutoff.
     * (Filtering inside the subquery would walk the whole index once nothing
     * is old enough.)
     */
    private static final String OLDEST_BY_ID = """
            DELETE FROM %1$s
            WHERE id IN (SELECT id FROM %1$s ORDER BY id LIMIT :batch)
              AND created_at < :cutoff
            """;

    private static final String SLA_BREACHES = """
            DELETE FROM sla_breaches
            WHERE id IN (SELECT id FROM sla_breaches WHERE detected_at < :cutoff LIMIT :batch)
            """;

    /* delivery_logs references completed_routes: a route's deliveries go first. */
    private static final String ROUTE_DELIVERIES = """
            DELETE FROM delivery_logs
            WHERE route_id IN (SELECT route_id FROM completed_routes
                               WHERE created_at < :cutoff
                               ORDER BY created_at LIMIT :batch)
            """;

    private static final String ROUTES = """
            DELETE FROM completed_routes
            WHERE route_id IN (SELECT r.route_id FROM completed_routes r
                               WHERE r.created_at < :cutoff
                                 AND NOT EXISTS (SELECT 1 FROM delivery_logs d WHERE d.route_id = r.route_id)
                               LIMIT :batch)
            """;

    private final DatabaseClient db;
    private final Duration interval;
    private final int batchSize;
    private final int routeBatchSize;
    private final int maxBatches;
    private final Duration gpsArchive;
    private final Duration deadLetters;
    private final Duration history;
    private final Counter deleted;
    private Disposable runner;

    public RetentionJob(DatabaseClient db, MeterRegistry meterRegistry,
            @Value("${milkrun.retention.interval:10m}") Duration interval,
            @Value("${milkrun.retention.batch-size:5000}") int batchSize,
            @Value("${milkrun.retention.max-batches:20}") int maxBatches,
            @Value("${milkrun.retention.gps-archive:24h}") Duration gpsArchive,
            @Value("${milkrun.retention.dead-letters:2d}") Duration deadLetters,
            @Value("${milkrun.retention.history:7d}") Duration history) {
        this.db = db;
        this.interval = interval;
        this.batchSize = batchSize;
        // A route has a few dozen delivery rows, so take fewer routes per batch.
        this.routeBatchSize = Math.max(1, batchSize / 50);
        this.maxBatches = maxBatches;
        this.gpsArchive = gpsArchive;
        this.deadLetters = deadLetters;
        this.history = history;
        this.deleted = Counter.builder("milkrun.retention.deleted")
                .description("Rows deleted by the retention job").register(meterRegistry);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        runner = Flux.interval(Duration.ofMinutes(1), interval)
                // A run slower than the interval is not stacked up behind itself.
                .onBackpressureDrop()
                .concatMap(tick -> runOnce()
                        .onErrorResume(e -> {
                            log.warn("Retention run failed, retrying next run: {}", e.getMessage());
                            return Mono.empty();
                        }), 1)
                .subscribe(null, e -> log.error("Retention job stopped", e));
    }

    @PreDestroy
    void stop() {
        if (runner != null) {
            runner.dispose();
        }
    }

    /** One pass over every table; completes with the number of rows deleted. */
    public Mono<Long> runOnce() {
        Instant now = Instant.now();
        return Flux.concat(
                        drain("gps_archive", () -> delete(OLDEST_BY_ID.formatted("gps_archive"),
                                now.minus(gpsArchive), batchSize), batchSize),
                        drain("dead_letter_log", () -> delete(OLDEST_BY_ID.formatted("dead_letter_log"),
                                now.minus(deadLetters), batchSize), batchSize),
                        drain("sla_breaches", () -> delete(SLA_BREACHES, now.minus(history), batchSize), batchSize),
                        drain("completed_routes and their deliveries", () -> delete(ROUTE_DELIVERIES, now.minus(history), routeBatchSize)
                                .then(delete(ROUTES, now.minus(history), routeBatchSize)), routeBatchSize))
                .reduce(0L, Long::sum);
    }

    /** Repeats a batch while it comes back full, up to max-batches. */
    private Mono<Long> drain(String table, Supplier<Mono<Long>> batch, int fullBatch) {
        return Mono.defer(batch)
                .expand(n -> n >= fullBatch ? batch.get() : Mono.empty())
                .take(maxBatches)
                .reduce(0L, Long::sum)
                .doOnNext(total -> {
                    if (total > 0) {
                        deleted.increment(total);
                        log.info("Retention: deleted {} old rows from {}", total, table);
                    }
                });
    }

    private Mono<Long> delete(String sql, Instant cutoff, int batch) {
        return db.sql(sql)
                .bind("cutoff", cutoff)
                .bind("batch", batch)
                .fetch()
                .rowsUpdated();
    }
}
