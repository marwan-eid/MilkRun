package com.milkrun.persistence;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

/**
 * Reactive repository for persisting dead letter events for audit/debugging.
 */
@Repository
public class DeadLetterRepository {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterRepository.class);

    private final DatabaseClient databaseClient;
    private final Counter dlqWrites;

    public DeadLetterRepository(DatabaseClient databaseClient, MeterRegistry meterRegistry) {
        this.databaseClient = databaseClient;
        this.dlqWrites = Counter.builder("milkrun.dlq.writes")
                .description("Events written to dead letter log")
                .register(meterRegistry);
    }

    /**
     * Log a dead letter event.
     */
    public Mono<Void> logDeadLetter(String originalTopic, String vanId,
                                      String eventPayload, String errorReason) {
        return databaseClient.sql("""
            INSERT INTO dead_letter_log (original_topic, van_id, event_payload, error_reason)
            VALUES (:topic, :vanId, :payload::jsonb, :reason)
            """)
                .bind("topic", originalTopic)
                .bind("vanId", vanId != null ? vanId : "unknown")
                .bind("payload", eventPayload)
                .bind("reason", errorReason)
                .then()
                .doOnSuccess(v -> dlqWrites.increment())
                .onErrorResume(e -> {
                    log.warn("Failed to write DLQ entry: {}", e.getMessage());
                    return Mono.empty();
                });
    }
}
