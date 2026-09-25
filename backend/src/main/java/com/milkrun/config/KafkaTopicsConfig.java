package com.milkrun.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Topics the backend needs. Spring's KafkaAdmin creates missing ones at startup
 * (and adds partitions if an existing topic has fewer). The simulator declares
 * the same topics, so whichever service starts first creates them.
 *
 * All van-keyed topics use the same partition count so that a van's GPS,
 * delivery and route-plan records land on the same partition number.
 */
@Configuration
public class KafkaTopicsConfig {

    static final int VAN_PARTITIONS = 10;

    @Bean
    NewTopic gpsEventsTopic(@Value("${milkrun.kafka.gps-topic}") String name) {
        return TopicBuilder.name(name).partitions(VAN_PARTITIONS).replicas(1).build();
    }

    @Bean
    NewTopic deliveryEventsTopic(@Value("${milkrun.kafka.delivery-topic}") String name) {
        return TopicBuilder.name(name).partitions(VAN_PARTITIONS).replicas(1).build();
    }

    /** Latest route plan per van; compaction keeps one record per key. */
    @Bean
    NewTopic routePlansTopic(@Value("${milkrun.kafka.route-plans-topic}") String name) {
        return TopicBuilder.name(name).partitions(VAN_PARTITIONS).replicas(1)
                .compact()
                .config("segment.ms", "600000")
                .build();
    }

    @Bean
    NewTopic dispatchEventsTopic(@Value("${milkrun.kafka.dispatch-topic}") String name) {
        return TopicBuilder.name(name).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic gpsDeadLetterTopic(@Value("${milkrun.kafka.dlq-topic}") String name) {
        return TopicBuilder.name(name).partitions(3).replicas(1).build();
    }
}
