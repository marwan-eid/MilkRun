package com.milkrun.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.CompletableFuture;

/**
 * Publishes dispatch messages to the dispatch-events topic.
 */
public interface DispatchPublisher {

    String TOPIC = "dispatch-events";

    /** Completes when the broker has acknowledged the record. */
    CompletableFuture<?> publish(String key, String payload);

    @Component
    class Kafka implements DispatchPublisher {
        private static final Logger log = LoggerFactory.getLogger(Kafka.class);

        private final KafkaTemplate<String, String> kafkaTemplate;

        public Kafka(KafkaTemplate<String, String> kafkaTemplate) {
            this.kafkaTemplate = kafkaTemplate;
        }

        /**
         * Creates the producer and fetches the topic's metadata up front. Otherwise
         * the first order after a restart pays for both inside the request, which
         * on a small VM outlasts the publish timeout.
         */
        @EventListener(ApplicationReadyEvent.class)
        public void warmUp() {
            Mono.fromRunnable(() -> kafkaTemplate.partitionsFor(TOPIC))
                    .subscribeOn(Schedulers.boundedElastic())
                    .subscribe(null, e -> log.warn("Dispatch producer warm-up failed: {}", e.toString()));
        }

        @Override
        public CompletableFuture<?> publish(String key, String payload) {
            return kafkaTemplate.send(TOPIC, key, payload);
        }
    }
}
