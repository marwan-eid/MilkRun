package com.milkrun.consumer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Forwards GPS records that could not be parsed to the gps-events-dlq topic,
 * unchanged, with headers saying why and where they came from. Keeping the
 * raw record in Kafka means it can be inspected and replayed once the
 * producer is fixed.
 */
@Component
public class KafkaDeadLetterPublisher {

    public static final String HEADER_ERROR = "x-dlq-error";
    public static final String HEADER_TOPIC = "x-dlq-original-topic";
    public static final String HEADER_PARTITION = "x-dlq-original-partition";
    public static final String HEADER_OFFSET = "x-dlq-original-offset";

    private static final Logger log = LoggerFactory.getLogger(KafkaDeadLetterPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String dlqTopic;
    private final Counter published;
    private final Counter failed;

    public KafkaDeadLetterPublisher(KafkaTemplate<String, String> kafkaTemplate, MeterRegistry meterRegistry,
            @Value("${milkrun.kafka.dlq-topic}") String dlqTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.dlqTopic = dlqTopic;
        this.published = Counter.builder("milkrun.kafka_dlq.published")
                .description("Records forwarded to the Kafka dead-letter topic").register(meterRegistry);
        this.failed = Counter.builder("milkrun.kafka_dlq.failed")
                .description("Records that could not be forwarded to the Kafka dead-letter topic").register(meterRegistry);
    }

    /** Completes when the broker has the record, or once retries have failed (logged and counted). */
    public Mono<Void> publish(ReceiverRecord<String, String> record, String error) {
        ProducerRecord<String, String> dlq = new ProducerRecord<>(dlqTopic, record.key(), record.value());
        dlq.headers()
                .add(HEADER_ERROR, bytes(error.length() > 500 ? error.substring(0, 500) : error))
                .add(HEADER_TOPIC, bytes(record.topic()))
                .add(HEADER_PARTITION, bytes(Integer.toString(record.partition())))
                .add(HEADER_OFFSET, bytes(Long.toString(record.offset())));
        return Mono.fromFuture(() -> kafkaTemplate.send(dlq))
                .retryWhen(Retry.backoff(3, Duration.ofMillis(200)))
                .doOnSuccess(r -> published.increment())
                .onErrorResume(e -> {
                    failed.increment();
                    log.error("Could not forward {}-{}@{} to {}: {}", record.topic(), record.partition(),
                            record.offset(), dlqTopic, e.toString());
                    return Mono.empty();
                })
                .then();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
