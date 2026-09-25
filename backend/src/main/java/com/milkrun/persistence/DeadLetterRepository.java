package com.milkrun.persistence;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

/**
 * Dead-letter log in PostgreSQL: events that could not be processed normally,
 * kept for audit and reconciliation. Failures are returned to the caller,
 * which decides whether to retry.
 */
@Repository
public class DeadLetterRepository {

    private static final com.fasterxml.jackson.databind.ObjectMapper JSON = new com.fasterxml.jackson.databind.ObjectMapper()
            .enable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    private final DatabaseClient databaseClient;
    private final Counter dlqWrites;

    public DeadLetterRepository(DatabaseClient databaseClient, MeterRegistry meterRegistry) {
        this.databaseClient = databaseClient;
        this.dlqWrites = Counter.builder("milkrun.dlq.writes")
                .description("Events written to the dead letter log")
                .register(meterRegistry);
    }

    /**
     * @param eventPayload JSON; anything that is not valid JSON is stored as a JSON string
     * @param reconciled   whether the event was also recovered elsewhere (e.g. archived)
     */
    public Mono<Void> logDeadLetter(String originalTopic, String vanId, String eventPayload,
            String errorReason, boolean reconciled) {
        return databaseClient.sql("""
                INSERT INTO dead_letter_log (original_topic, van_id, event_payload, error_reason,
                                             reconciled, reconciled_at)
                VALUES (:topic, :vanId,
                        CAST(:payload AS jsonb),
                        :reason, :reconciled, CASE WHEN :reconciled THEN now() END)
                """)
                .bind("topic", originalTopic)
                .bind("vanId", vanId != null ? vanId : "unknown")
                .bind("payload", asJson(eventPayload))
                .bind("reason", errorReason.length() > 255 ? errorReason.substring(0, 255) : errorReason)
                .bind("reconciled", reconciled)
                .then()
                .doOnSuccess(v -> dlqWrites.increment());
    }

    /** The payload itself if it is valid JSON, otherwise the payload as a JSON string. */
    static String asJson(String s) {
        String text = s != null ? s : "";
        try {
            if (!text.isBlank()) {
                JSON.readTree(text);
                return text;
            }
        } catch (Exception notJson) {
            // fall through
        }
        try {
            return JSON.writeValueAsString(text);
        } catch (Exception e) {
            return "\"\"";
        }
    }
}
