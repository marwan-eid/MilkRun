package com.milkrun.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.milkrun.model.GpsEvent;
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

    @Value("${milkrun.kafka.consumer-group}")
    private String consumerGroup;

    @Bean
    public ReceiverOptions<String, String> gpsReceiverOptions() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);

        return ReceiverOptions.<String, String>create(props)
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
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        return ReceiverOptions.<String, String>create(props)
                .subscription(Collections.singleton("delivery-events"));
    }

    @Bean
    public KafkaReceiver<String, String> deliveryKafkaReceiver(
            @Qualifier("deliveryReceiverOptions") ReceiverOptions<String, String> options) {
        return KafkaReceiver.create(options);
    }
}
