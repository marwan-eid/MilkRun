package com.milkrun.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Reactive Kafka consumer configuration.
 * Creates a KafkaReceiver<String, String> that consumes from the gps-events
 * topic.
 */
@Configuration
public class KafkaConsumerConfig {

    @Value("${milkrun.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${milkrun.kafka.gps-topic}")
    private String gpsTopic;

    @Value("${milkrun.kafka.delivery-topic}")
    private String deliveryTopic;

    @Value("${milkrun.kafka.auto-offset-reset:latest}")
    private String autoOffsetReset;

    @Value("${milkrun.kafka.commit-interval:5s}")
    private java.time.Duration commitInterval;

    @Value("${milkrun.kafka.consumer-group}")
    private String consumerGroup;

    @Bean
    public ReceiverOptions<String, String> gpsReceiverOptions() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);

        // Offsets are acknowledged when an event leaves the pipeline, which is
        // out of order (events wait in the reorder buffer). Deferred commits keep
        // each partition's committed offset below the oldest unfinished record;
        // past this many pending acknowledgements the consumer pauses.
        return ReceiverOptions.<String, String>create(props)
                .maxDeferredCommits(5000)
                .commitInterval(commitInterval)
                .subscription(Collections.singleton(gpsTopic));
    }

    @Bean
    public KafkaReceiver<String, String> gpsKafkaReceiver(
            @Qualifier("gpsReceiverOptions") ReceiverOptions<String, String> options) {
        return KafkaReceiver.create(options);
    }

    @Bean
    public ReceiverOptions<String, String> deliveryReceiverOptions() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup + "-delivery");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return ReceiverOptions.<String, String>create(props)
                .commitInterval(commitInterval)
                .subscription(Collections.singleton(deliveryTopic));
    }

    @Bean
    public KafkaReceiver<String, String> deliveryKafkaReceiver(
            @Qualifier("deliveryReceiverOptions") ReceiverOptions<String, String> options) {
        return KafkaReceiver.create(options);
    }
}
