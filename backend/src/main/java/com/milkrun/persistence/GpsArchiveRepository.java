package com.milkrun.persistence;

import com.milkrun.model.GpsEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.r2dbc.spi.Statement;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.concurrent.Queues;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Writes a sampled GPS track per van to gps_archive (PostGIS points), which is
 * what route distances and path replays are computed from.
 *
 * Archiving is best effort and never slows the live pipeline: points go into
 * a bounded queue and are inserted in batches; when the queue is full (the
 * database is slow or down) points are dropped and counted. Inserts are
 * idempotent on event_id, so replayed Kafka records are not archived twice.
 */
@Repository
public class GpsArchiveRepository {

    private static final Logger log = LoggerFactory.getLogger(GpsArchiveRepository.class);

    private static final String INSERT = """
            INSERT INTO gps_archive (event_id, van_id, route_id, location, speed_kmh,
                                     heading, battery_pct, device_timestamp, ingestion_timestamp)
            VALUES ($1, $2, $3, ST_SetSRID(ST_MakePoint($4, $5), 4326), $6, $7, $8, $9, $10)
            ON CONFLICT (event_id) DO NOTHING
            """;

    private final DatabaseClient databaseClient;
    private final Duration minInterval;
    private final Sinks.Many<GpsEvent> queue = Sinks.many().unicast()
            .onBackpressureBuffer(Queues.<GpsEvent>get(10_000).get());
    private final ConcurrentHashMap<String, Instant> lastArchived = new ConcurrentHashMap<>();
    private final Counter archived;
    private final Counter dropped;
    private Disposable writer;

    public GpsArchiveRepository(DatabaseClient databaseClient, MeterRegistry meterRegistry,
            @Value("${milkrun.archive.min-interval:2s}") Duration minInterval) {
        this.databaseClient = databaseClient;
        this.minInterval = minInterval;
        this.archived = Counter.builder("milkrun.gps.archived")
                .description("GPS points written to gps_archive").register(meterRegistry);
        this.dropped = Counter.builder("milkrun.gps.archive_dropped")
                .description("GPS points not archived because the write queue was full").register(meterRegistry);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        writer = queue.asFlux()
                .bufferTimeout(200, Duration.ofSeconds(1))
                .concatMap(batch -> insert(batch)
                        .retryWhen(Retry.backoff(3, Duration.ofMillis(500)))
                        .onErrorResume(e -> {
                            log.warn("Dropping {} archive points: {}", batch.size(), e.getMessage());
                            dropped.increment(batch.size());
                            return Mono.empty();
                        }))
                .subscribe();
    }

    @PreDestroy
    void stop() {
        if (writer != null) {
            writer.dispose();
        }
    }

    /**
     * Queues the point if the van's previous archived point is at least
     * min-interval older (device time), and always for the final RETURNED
     * point, so each route's track ends at the hub. Synchronized: the sink
     * accepts one emitter at a time.
     */
    public synchronized void offer(GpsEvent event) {
        Instant previous = lastArchived.get(event.vanId());
        boolean due = previous == null
                || !event.deviceTimestamp().isBefore(previous.plus(minInterval))
                || event.status() == com.milkrun.model.VanStatus.RETURNED;
        if (!due) {
            return;
        }
        if (queue.tryEmitNext(event).isSuccess()) {
            lastArchived.put(event.vanId(), event.deviceTimestamp());
        } else {
            dropped.increment();
        }
    }

    /**
     * Writes one point immediately, bypassing sampling (used to reconcile late
     * events into the track). Idempotent on event_id.
     */
    public Mono<Void> insertNow(GpsEvent event) {
        return insert(List.of(event));
    }

    private Mono<Void> insert(List<GpsEvent> batch) {
        return databaseClient.inConnection(connection -> {
            Statement statement = connection.createStatement(INSERT);
            for (int i = 0; i < batch.size(); i++) {
                GpsEvent e = batch.get(i);
                if (i > 0) {
                    statement.add();
                }
                statement.bind("$1", e.eventId())
                        .bind("$2", e.vanId())
                        .bind("$3", e.routeId())
                        .bind("$4", e.location().longitude())
                        .bind("$5", e.location().latitude())
                        .bind("$6", e.speedKmh())
                        .bind("$7", e.headingDegrees())
                        .bind("$8", e.batteryPct())
                        .bind("$9", e.deviceTimestamp())
                        .bind("$10", e.ingestionTimestamp() != null ? e.ingestionTimestamp() : e.deviceTimestamp());
            }
            return Flux.from(statement.execute())
                    .flatMap(result -> result.getRowsUpdated())
                    .reduce(0L, (sum, n) -> sum + n.longValue())
                    .doOnNext(archived::increment)
                    .then();
        });
    }
}
