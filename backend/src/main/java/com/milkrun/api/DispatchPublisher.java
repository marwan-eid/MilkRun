package com.milkrun.api;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

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
        private final KafkaTemplate<String, String> kafkaTemplate;

        public Kafka(KafkaTemplate<String, String> kafkaTemplate) {
            this.kafkaTemplate = kafkaTemplate;
        }

        @Override
        public CompletableFuture<?> publish(String key, String payload) {
            return kafkaTemplate.send(TOPIC, key, payload);
        }
    }
}
