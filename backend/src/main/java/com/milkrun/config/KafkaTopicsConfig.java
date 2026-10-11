package com.milkrun.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Topics the backend needs. Spring's KafkaAdmin creates missing ones at startup,
 * adds partitions if an existing topic has fewer, and applies the configs below
 * to topics that already exist (spring.kafka.admin.modify-topic-configs). The simulator declares
 * the same topics, so whichever service starts first creates them.
 *
 * All van-keyed topics use the same partition count so that a van's GPS,
 * delivery and route-plan records land on the same partition number.
 */
@Configuration
public class KafkaTopicsConfig {

    static final int VAN_PARTITIONS = 10;

    /**
     * GPS pings are consumed within seconds and their track is archived in
     * PostgreSQL, so the topic only needs a short replay buffer. Without a limit
     * it keeps Kafka's default 7 days, which once filled the demo VM's disk.
     * retention.bytes is per partition.
     */
    static final String GPS_RETENTION_MS = "3600000";
    static final String GPS_RETENTION_BYTES = "104857600";

    @Bean
    NewTopic gpsEventsTopic(@Value("${milkrun.kafka.gps-topic}") String name) {
        return TopicBuilder.name(name).partitions(VAN_PARTITIONS).replicas(1)
                .config("retention.ms", GPS_RETENTION_MS)
                .config("retention.bytes", GPS_RETENTION_BYTES)
                .config("segment.bytes", "52428800")
                .config("segment.ms", "600000")
                .build();
    }

    @Bean
    NewTopic deliveryEventsTopic(@Value("${milkrun.kafka.delivery-topic}") String name) {
        return TopicBuilder.name(name).partitions(VAN_PARTITIONS).replicas(1)
                .config("retention.ms", "86400000")
                .config("retention.bytes", "52428800")
                .config("segment.bytes", "26214400")
                .build();
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

    /** Shared van states for multi-instance deployments; a few minutes is plenty. */
    @Bean
    @ConditionalOnProperty(name = "milkrun.fanout.mode", havingValue = "kafka")
    NewTopic vanStateTopic(@Value("${milkrun.kafka.van-state-topic}") String name) {
        return TopicBuilder.name(name).partitions(VAN_PARTITIONS).replicas(1)
                .config("retention.ms", "600000")
                .config("segment.ms", "300000")
                .build();
    }

    @Bean
    NewTopic gpsDeadLetterTopic(@Value("${milkrun.kafka.dlq-topic}") String name) {
        return TopicBuilder.name(name).partitions(3).replicas(1)
                .config("retention.ms", "604800000")
                .config("retention.bytes", "52428800")
                .config("segment.bytes", "26214400")
                .build();
    }
}
