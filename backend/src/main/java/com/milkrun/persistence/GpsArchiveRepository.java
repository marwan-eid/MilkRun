package com.milkrun.persistence;

import com.milkrun.model.GpsEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

/**
 * Reactive repository for persisting GPS events to the gps_archive table.
 * Samples events (1 in 5) to avoid overwhelming the DB with high-frequency writes.
 */
@Repository
public class GpsArchiveRepository {

    private static final Logger log = LoggerFactory.getLogger(GpsArchiveRepository.class);

    private final DatabaseClient databaseClient;
    private final Counter archivedEvents;
    private long sampleCounter = 0;

    public GpsArchiveRepository(DatabaseClient databaseClient, MeterRegistry meterRegistry) {
        this.databaseClient = databaseClient;
        this.archivedEvents = Counter.builder("milkrun.gps.archived")
                .description("GPS events written to archive")
                .register(meterRegistry);
    }

    /**
     * Archive a GPS event (sampled: 1 in 5 events).
     */
    public Mono<Void> archiveSampled(GpsEvent event) {
        sampleCounter++;
        if (sampleCounter % 5 != 0) {
            return Mono.empty();
        }

        return databaseClient.sql("""
            INSERT INTO gps_archive (event_id, van_id, route_id, location, speed_kmh,
                                     heading, battery_pct, device_timestamp, ingestion_timestamp)
            VALUES (:eventId, :vanId, :routeId,
                    ST_SetSRID(ST_MakePoint(:longitude, :latitude), 4326),
                    :speedKmh, :heading, :batteryPct, :deviceTs, :ingestionTs)
            """)
                .bind("eventId", event.eventId())
                .bind("vanId", event.vanId())
                .bind("routeId", event.routeId())
                .bind("longitude", event.location().longitude())
                .bind("latitude", event.location().latitude())
                .bind("speedKmh", event.speedKmh())
                .bind("heading", event.headingDegrees())
                .bind("batteryPct", event.batteryPct())
                .bind("deviceTs", event.deviceTimestamp())
                .bind("ingestionTs", event.ingestionTimestamp() != null ?
                        event.ingestionTimestamp() : event.deviceTimestamp())
                .then()
                .doOnSuccess(v -> archivedEvents.increment())
                .onErrorResume(e -> {
                    log.warn("Failed to archive GPS event: {}", e.getMessage());
                    return Mono.empty();
                });
    }
}
