package com.milkrun.fleet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.milkrun.consumer.GpsEventPipeline;
import com.milkrun.model.VanState;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Fleet view for running several backend instances.
 *
 * Kafka assigns each instance a share of the gps-events partitions, so each
 * computes state only for its own vans. Every instance publishes those states
 * to the van-state topic (at most one per van per publish interval, plus any
 * status change) and reads the whole topic back with a consumer group of its
 * own, so each keeps a replica of the entire fleet: any instance can serve
 * the SSE stream, the REST snapshot, dispatch and analytics.
 *
 * The replica starts empty and fills within one publish interval, since
 * every van reports twice a second; the topic only keeps a few minutes.
 */
@Component
@ConditionalOnProperty(name = "milkrun.fanout.mode", havingValue = "kafka")
public class KafkaFleetView implements FleetView {

    private static final Logger log = LoggerFactory.getLogger(KafkaFleetView.class);

    private final GpsEventPipeline pipeline;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String bootstrapServers;
    private final String topic;
    private final Duration publishInterval;

    private final ConcurrentHashMap<String, VanState> replica = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, VanState> lastPublished = new ConcurrentHashMap<>();
    private final Sinks.Many<VanState> updates = Sinks.many().multicast().directBestEffort();
    private final Counter published;
    private final Counter publishFailures;
    private final Counter received;

    private Disposable publishing;
    private Disposable consuming;

    public KafkaFleetView(GpsEventPipeline pipeline, KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper, MeterRegistry meterRegistry,
            @Value("${milkrun.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${milkrun.kafka.van-state-topic}") String topic,
            @Value("${milkrun.fanout.publish-interval-ms:500}") long publishIntervalMs) {
        this.pipeline = pipeline;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.publishInterval = Duration.ofMillis(publishIntervalMs);
        this.published = Counter.builder("milkrun.fanout.published")
                .description("Van states published to the van-state topic").register(meterRegistry);
        this.publishFailures = Counter.builder("milkrun.fanout.publish_failures")
                .description("Van states that could not be published").register(meterRegistry);
        this.received = Counter.builder("milkrun.fanout.received")
                .description("Van states read from the van-state topic").register(meterRegistry);
        Gauge.builder("milkrun.fanout.replica_size", replica, Map::size)
                .description("Vans in this instance's fleet replica").register(meterRegistry);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        publishing = pipeline.rawVanStateStream()
                .filter(this::due)
                .flatMap(this::publish, 16)
                .subscribe();

        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "milkrun-fleet-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consuming = KafkaReceiver.create(ReceiverOptions.<String, String>create(props).subscription(List.of(topic)))
                .receive()
                .doOnNext(record -> apply(record.value()))
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1)).maxBackoff(Duration.ofSeconds(30))
                        .doBeforeRetry(s -> log.warn("Fleet replica consumer restarting: {}", s.failure().toString())))
                .subscribe();
        log.info("Fleet view: publishing and replicating van states via {}", topic);
    }

    @PreDestroy
    void stop() {
        if (publishing != null) publishing.dispose();
        if (consuming != null) consuming.dispose();
    }

    /** Publish if the van's last published state is older than the interval or had another status. */
    boolean due(VanState state) {
        VanState previous = lastPublished.get(state.vanId());
        boolean due = previous == null
                || previous.status() != state.status()
                || !state.lastUpdated().isBefore(previous.lastUpdated().plus(publishInterval));
        if (due) {
            lastPublished.put(state.vanId(), state);
        }
        return due;
    }

    private Mono<Void> publish(VanState state) {
        return Mono.fromCallable(() -> objectMapper.writeValueAsString(state))
                .flatMap(json -> Mono.fromFuture(() -> kafkaTemplate.send(topic, state.vanId(), json)))
                .doOnSuccess(r -> published.increment())
                .onErrorResume(e -> {
                    publishFailures.increment();
                    log.debug("Could not publish state of {}: {}", state.vanId(), e.toString());
                    return Mono.empty();
                })
                .then();
    }

    /** Applies a replicated state unless a newer one for the van is already held. */
    void apply(String json) {
        VanState state;
        try {
            state = objectMapper.readValue(json, VanState.class);
        } catch (Exception e) {
            log.warn("Unreadable van state on {}: {}", topic, e.getMessage());
            return;
        }
        received.increment();
        boolean[] newer = { false };
        replica.compute(state.vanId(), (id, current) -> {
            if (current == null || !state.lastUpdated().isBefore(current.lastUpdated())) {
                newer[0] = true;
                return state;
            }
            return current;
        });
        if (newer[0]) {
            updates.tryEmitNext(state);
        }
    }

    @Override
    public Collection<VanState> all() {
        return replica.values();
    }

    @Override
    public Optional<VanState> get(String vanId) {
        return Optional.ofNullable(replica.get(vanId));
    }

    @Override
    public Flux<VanState> updates() {
        return updates.asFlux();
    }
}
