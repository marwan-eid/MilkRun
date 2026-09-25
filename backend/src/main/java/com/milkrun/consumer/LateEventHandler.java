package com.milkrun.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.milkrun.persistence.DeadLetterRepository;
import com.milkrun.pipeline.IngestedGps;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

/**
 * GPS events that arrive after newer events of the same van have already been
 * released are too late to put in order. They are recorded in the dead-letter
 * log for audit; their Kafka offset is acknowledged once that write is done.
 */
@Service
public class LateEventHandler {

    private static final Logger log = LoggerFactory.getLogger(LateEventHandler.class);

    private final DeadLetterRepository deadLetters;
    private final ObjectMapper objectMapper;
    private final Counter writeFailures;

    public LateEventHandler(DeadLetterRepository deadLetters, ObjectMapper objectMapper, MeterRegistry meterRegistry) {
        this.deadLetters = deadLetters;
        this.objectMapper = objectMapper;
        this.writeFailures = Counter.builder("milkrun.dlq.write_failures")
                .description("Dead-letter writes that failed after retries")
                .register(meterRegistry);
    }

    public Mono<Void> handle(IngestedGps item) {
        return Mono.fromCallable(() -> objectMapper.writeValueAsString(item.event()))
                .flatMap(payload -> deadLetters.logDeadLetter("gps-events", item.event().vanId(), payload,
                        "LATE_ARRIVAL", false))
                .retryWhen(Retry.backoff(3, Duration.ofMillis(200)))
                .onErrorResume(e -> {
                    // A late position is not worth stalling the partition for.
                    writeFailures.increment();
                    log.warn("Could not dead-letter late event of {}: {}", item.event().vanId(), e.getMessage());
                    return Mono.empty();
                })
                .doFinally(signal -> item.ack());
    }
}
