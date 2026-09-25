package com.milkrun.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.milkrun.persistence.DeadLetterRepository;
import com.milkrun.persistence.GpsArchiveRepository;
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
 * GPS events that arrive after newer events of the same van were already
 * released (typically a backlog uploaded after a dead zone) are too late for
 * the live view. They are still real positions, so they are reconciled into
 * the GPS archive, which keeps the van's recorded track complete, and logged
 * in the dead-letter log as reconciled. The Kafka offset is acknowledged
 * afterwards.
 */
@Service
public class LateEventHandler {

    private static final Logger log = LoggerFactory.getLogger(LateEventHandler.class);

    private final DeadLetterRepository deadLetters;
    private final GpsArchiveRepository archive;
    private final ObjectMapper objectMapper;
    private final Counter reconciled;
    private final Counter writeFailures;

    public LateEventHandler(DeadLetterRepository deadLetters, GpsArchiveRepository archive,
            ObjectMapper objectMapper, MeterRegistry meterRegistry) {
        this.deadLetters = deadLetters;
        this.archive = archive;
        this.objectMapper = objectMapper;
        this.reconciled = Counter.builder("milkrun.events.late_reconciled")
                .description("Late GPS events written into the archive")
                .register(meterRegistry);
        this.writeFailures = Counter.builder("milkrun.dlq.write_failures")
                .description("Dead-letter writes that failed after retries")
                .register(meterRegistry);
    }

    public Mono<Void> handle(IngestedGps item) {
        Retry retry = Retry.backoff(3, Duration.ofMillis(200));
        Mono<Boolean> archived = archive.insertNow(item.event())
                .retryWhen(retry)
                .thenReturn(true)
                .doOnNext(ok -> reconciled.increment())
                .onErrorResume(e -> {
                    log.warn("Could not archive late event of {}: {}", item.event().vanId(), e.getMessage());
                    return Mono.just(false);
                });

        return archived
                .flatMap(ok -> Mono.fromCallable(() -> objectMapper.writeValueAsString(item.event()))
                        .flatMap(payload -> deadLetters.logDeadLetter("gps-events", item.event().vanId(), payload,
                                "LATE_ARRIVAL", ok))
                        .retryWhen(retry))
                .onErrorResume(e -> {
                    // A late position is not worth stalling the partition for.
                    writeFailures.increment();
                    log.warn("Could not dead-letter late event of {}: {}", item.event().vanId(), e.getMessage());
                    return Mono.empty();
                })
                .doFinally(signal -> item.ack());
    }
}
