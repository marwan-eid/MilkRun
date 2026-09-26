package com.milkrun;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.milkrun.consumer.GpsEventPipeline;
import com.milkrun.consumer.KafkaDeadLetterPublisher;
import com.milkrun.model.DeliveryEvent;
import com.milkrun.model.DeliveryEventType;
import com.milkrun.model.GpsEvent;
import com.milkrun.model.Location;
import com.milkrun.model.RoutePlan;
import com.milkrun.model.VanState;
import com.milkrun.model.VanStatus;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import reactor.core.Disposable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end pipeline tests: the whole Spring application against real Kafka
 * and PostGIS containers.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers(disabledWithoutDocker = true)
class PipelineIntegrationTest {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.7.0");

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(
            DockerImageName.parse("postgis/postgis:16-3.4").asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("milkrun.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.kafka.producer.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.r2dbc.url", () -> "r2dbc:postgresql://%s:%d/%s".formatted(
                POSTGRES.getHost(), POSTGRES.getMappedPort(5432), POSTGRES.getDatabaseName()));
        registry.add("spring.r2dbc.username", POSTGRES::getUsername);
        registry.add("spring.r2dbc.password", POSTGRES::getPassword);
        registry.add("spring.flyway.url", POSTGRES::getJdbcUrl);
        registry.add("milkrun.kafka.auto-offset-reset", () -> "earliest");
        registry.add("milkrun.kafka.commit-interval", () -> "200ms");
        registry.add("milkrun.pipeline.reorder-buffer-grace-ms", () -> "2000");
        registry.add("milkrun.archive.min-interval", () -> "0s");
    }

    static final ObjectMapper JSON = new ObjectMapper().findAndRegisterModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    static KafkaProducer<String, String> producer;

    @Autowired
    GpsEventPipeline gpsPipeline;

    @Autowired
    MeterRegistry meters;

    @Autowired
    com.milkrun.engine.RoutePlanStore store;

    @Autowired
    com.milkrun.engine.EtaEngine etaEngine;

    @Autowired
    org.springframework.test.web.reactive.server.WebTestClient webClient;

    @BeforeAll
    static void startProducer() {
        producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
    }

    @AfterAll
    static void stopProducer() {
        producer.close();
    }

    // ═══════════════════ Helpers ═══════════════════

    static RecordMetadata send(String topic, String key, Object value) throws Exception {
        String payload = value instanceof String s ? s : JSON.writeValueAsString(value);
        return producer.send(new ProducerRecord<>(topic, key, payload)).get();
    }

    /** Route north along lon 4.85 (outside every zone) with one stop at vertex 20 of 41. */
    static RoutePlan plan(String van, String route) {
        return plan(van, route, 4.85);
    }

    static RoutePlan plan(String van, String route, double lon) {
        double[][] w = new double[41][];
        for (int i = 0; i < w.length; i++) {
            w[i] = new double[] { 52.300 + i * 0.0005, lon };
        }
        Instant deadline = Instant.now().plusSeconds(600);
        return new RoutePlan(route, van, 0, Instant.now(), 4, 25,
                List.of(new RoutePlan.Stop(0, "c0", new Location(52.310, lon), deadline, deadline.minusSeconds(60), 1, 20)),
                w);
    }

    static GpsEvent gps(String van, String route, long seq, Instant at, int vertex) {
        return gps(van, route, seq, at, vertex, 4.85);
    }

    static GpsEvent gps(String van, String route, long seq, Instant at, int vertex, double lon) {
        return new GpsEvent(UUID.randomUUID(), van, seq, at, at, new Location(52.300 + vertex * 0.0005, lon),
                25, 0, 90, route, 0, 1, VanStatus.EN_ROUTE);
    }

    static DeliveryEvent delivery(String van, String route, int stop, int totalStops, DeliveryEventType type,
            Instant at, Instant deadline) {
        return new DeliveryEvent(UUID.randomUUID(), van, route, stop, "cust-" + stop, type, at,
                new Location(52.31, 4.85), type == DeliveryEventType.DELIVERY_COMPLETED ? 2 : 0, 40, deadline,
                totalStops, null);
    }

    static long queryLong(String sql) throws Exception {
        try (Connection c = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            return rs.next() ? rs.getLong(1) : -1;
        }
    }

    static String queryString(String sql) throws Exception {
        try (Connection c = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    static long committedOffset(String group, TopicPartition tp) throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            OffsetAndMetadata o = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get().get(tp);
            return o == null ? -1 : o.offset();
        }
    }

    double counter(String name) {
        var c = meters.find(name).counter();
        return c == null ? 0 : c.count();
    }

    // ═══════════════════ GPS path ═══════════════════

    @Test
    void gpsEventsAreDedupedReorderedAndArchived() throws Exception {
        String van = "van-it-gps";
        String route = "route-it-gps";
        send("route-plans", van, plan(van, route));
        await().atMost(Duration.ofSeconds(30)).until(() -> counter("milkrun.route_plans.received") >= 1);

        List<VanState> seen = new CopyOnWriteArrayList<>();
        Disposable sub = gpsPipeline.rawVanStateStream().filter(s -> s.vanId().equals(van)).subscribe(seen::add);
        try {
            // 20 pings, sent shuffled in groups of 5, with three of them sent twice (retry duplicates)
            Instant t0 = Instant.now();
            List<GpsEvent> events = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                events.add(gps(van, route, 1000 + i, t0.plusMillis(i * 20L), i));
            }
            List<GpsEvent> sendOrder = new ArrayList<>();
            for (int g = 0; g < 20; g += 5) {
                List<GpsEvent> group = new ArrayList<>(events.subList(g, g + 5));
                Collections.shuffle(group);
                sendOrder.addAll(group);
            }
            for (int dup : List.of(3, 7, 15)) {
                GpsEvent e = events.get(dup);
                sendOrder.add(new GpsEvent(UUID.randomUUID(), e.vanId(), e.sequenceNumber(), e.deviceTimestamp(),
                        e.ingestionTimestamp(), e.location(), e.speedKmh(), e.headingDegrees(), e.batteryPct(),
                        e.routeId(), e.currentStopIndex(), e.totalStops(), e.status()));
            }
            double dedupBefore = counter("milkrun.events.deduplicated");
            for (GpsEvent e : sendOrder) {
                send("gps-events", van, e);
            }

            await().atMost(Duration.ofSeconds(30)).until(() -> seen.size() >= 20);
            Thread.sleep(500);
            assertEquals(20, seen.size(), "each ping exactly once");
            for (int i = 1; i < seen.size(); i++) {
                assertTrue(seen.get(i).location().latitude() > seen.get(i - 1).location().latitude(),
                        "released in device-time order");
            }
            VanState last = seen.get(seen.size() - 1);
            assertEquals(VanState.EtaMethod.ROUTE, last.etaMethod());
            assertEquals(VanState.SlaRisk.NONE, last.slaRisk());
            assertEquals(3, counter("milkrun.events.deduplicated") - dedupBefore, 0.001);

            await().atMost(Duration.ofSeconds(15)).until(() ->
                    queryLong("SELECT count(*) FROM gps_archive WHERE route_id = '" + route + "'") == 20);
        } finally {
            sub.dispose();
        }

        // An event older than what was already released is not shown live, but it is
        // reconciled into the archive and logged as a reconciled dead letter
        GpsEvent late = gps(van, route, 999, Instant.now().minusSeconds(60), 0);
        send("gps-events", van, late);
        await().atMost(Duration.ofSeconds(15)).until(() -> queryLong(
                "SELECT count(*) FROM dead_letter_log WHERE van_id = '" + van
                        + "' AND error_reason = 'LATE_ARRIVAL' AND reconciled AND reconciled_at IS NOT NULL") == 1);
        assertEquals(1, queryLong("SELECT count(*) FROM gps_archive WHERE event_id = '" + late.eventId() + "'"));
    }

    @Test
    void gpsOffsetIsCommittedOnlyAfterTheEventIsReleased() throws Exception {
        String van = "van-it-commit";
        // Device time 4 s ahead: the event waits in the reorder buffer for ~5 s
        RecordMetadata held = send("gps-events", van, gps(van, "route-it-commit", 1, Instant.now().plusSeconds(4), 1));
        TopicPartition tp = new TopicPartition(held.topic(), held.partition());

        Instant until = Instant.now().plusSeconds(3);
        while (Instant.now().isBefore(until)) {
            assertTrue(committedOffset("milkrun-engine", tp) <= held.offset(),
                    "offset committed while the event was still buffered");
            Thread.sleep(250);
        }
        await().atMost(Duration.ofSeconds(20)).until(() -> committedOffset("milkrun-engine", tp) > held.offset());
    }

    @Test
    void malformedGpsRecordsGoToTheDeadLetterTopic() throws Exception {
        double before = counter("milkrun.events.deserialization_errors");
        RecordMetadata bad = send("gps-events", "van-it-bad", "{not json");
        TopicPartition tp = new TopicPartition(bad.topic(), bad.partition());
        await().atMost(Duration.ofSeconds(20)).until(() -> counter("milkrun.events.deserialization_errors") > before);

        try (KafkaConsumer<String, String> dlq = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "it-dlq-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            dlq.subscribe(List.of("gps-events-dlq"));
            List<ConsumerRecord<String, String>> found = new ArrayList<>();
            await().atMost(Duration.ofSeconds(20)).until(() -> {
                dlq.poll(Duration.ofMillis(500)).forEach(r -> {
                    if ("van-it-bad".equals(r.key())) found.add(r);
                });
                return !found.isEmpty();
            });
            ConsumerRecord<String, String> r = found.get(0);
            assertEquals("{not json", r.value(), "forwarded unchanged");
            assertEquals("gps-events", header(r, KafkaDeadLetterPublisher.HEADER_TOPIC));
            assertEquals(Long.toString(bad.offset()), header(r, KafkaDeadLetterPublisher.HEADER_OFFSET));
            assertTrue(header(r, KafkaDeadLetterPublisher.HEADER_ERROR).contains("JsonParseException"));
        }
        await().atMost(Duration.ofSeconds(20)).until(() -> committedOffset("milkrun-engine", tp) > bad.offset());
    }

    private static String header(ConsumerRecord<String, String> r, String name) {
        var h = r.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), java.nio.charset.StandardCharsets.UTF_8);
    }

    // ═══════════════════ Dispatch ═══════════════════

    @Test
    void dispatchAssignsAnOrderToAVanAndPublishesIt() throws Exception {
        String van = "van-it-dispatch";
        String route = "route-it-dispatch";
        // A corridor of its own (lon 4.95), away from the other tests' vans
        send("route-plans", van, plan(van, route, 4.95));
        await().atMost(Duration.ofSeconds(30)).until(() -> store.get(van, route).isPresent());
        send("gps-events", van, gps(van, route, 1, Instant.now(), 5, 4.95));
        await().atMost(Duration.ofSeconds(30)).until(() -> etaEngine.getAllVanStates().containsKey(van));

        // A page on another origin is still refused by CORS...
        webClient.post().uri("/api/dispatch")
                .header("Origin", "https://elsewhere.example")
                .header("X-Forwarded-Proto", "https")
                .header("X-Forwarded-Host", "dashboard.example")
                .header("Content-Type", "application/json")
                .bodyValue("{\"latitude\":52.3070,\"longitude\":4.9510}")
                .exchange()
                .expectStatus().isForbidden();

        // ...while the dashboard behind the proxies is recognised as same-origin
        // (browsers send Origin on every POST, and that origin is not on the allow-list).
        var response = webClient.post().uri("/api/dispatch")
                .header("Origin", "https://dashboard.example")
                .header("X-Forwarded-Proto", "https")
                .header("X-Forwarded-Host", "dashboard.example")
                .header("X-Forwarded-For", "203.0.113.50, 172.18.0.6")
                .header("Content-Type", "application/json")
                .bodyValue("{\"latitude\":52.3070,\"longitude\":4.9510}")
                .exchange()
                .expectStatus().isAccepted()
                .expectBody(Map.class).returnResult().getResponseBody();
        assertNotNull(response);
        assertEquals(van, response.get("van_id"));
        assertEquals(0, response.get("insert_before"), "before the van's only stop");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "it-dispatch-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            consumer.subscribe(List.of("dispatch-events"));
            List<ConsumerRecord<String, String>> found = new ArrayList<>();
            await().atMost(Duration.ofSeconds(20)).until(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(r -> {
                    if (van.equals(r.key())) found.add(r);
                });
                return !found.isEmpty();
            });
            Map<?, ?> message = JSON.readValue(found.get(0).value(), Map.class);
            assertEquals(route, message.get("route_id"));
            assertEquals(response.get("order_id"), message.get("order_id"));
        }
    }

    // ═══════════════════ Delivery path ═══════════════════

    @Test
    void deliveryEventsAreIdempotentAndCompleteTheRoute() throws Exception {
        String van = "van-it-delivery";
        String route = "route-it-delivery";
        Instant t = Instant.now();
        List<DeliveryEvent> events = List.of(
                // stop 0: arrives 30 s after its deadline, delivered
                delivery(van, route, 0, 2, DeliveryEventType.ARRIVAL, t, t.minusSeconds(30)),
                delivery(van, route, 0, 2, DeliveryEventType.DELIVERY_COMPLETED, t.plusSeconds(10), t.minusSeconds(30)),
                delivery(van, route, 0, 2, DeliveryEventType.DEPARTURE, t.plusSeconds(10), t.minusSeconds(30)),
                // stop 1: on time, customer not home
                delivery(van, route, 1, 2, DeliveryEventType.ARRIVAL, t.plusSeconds(60), t.plusSeconds(300)),
                delivery(van, route, 1, 2, DeliveryEventType.DELIVERY_FAILED, t.plusSeconds(70), t.plusSeconds(300)),
                delivery(van, route, 1, 2, DeliveryEventType.DEPARTURE, t.plusSeconds(70), t.plusSeconds(300)));

        double receivedBefore = counter("milkrun.delivery.events.received");
        // Everything twice: the second copy is what a replay after a crash looks like
        for (int round = 0; round < 2; round++) {
            for (DeliveryEvent e : events) {
                send("delivery-events", van, e);
            }
        }
        await().atMost(Duration.ofSeconds(30)).until(() -> counter("milkrun.delivery.events.received") - receivedBefore >= 12);
        await().atMost(Duration.ofSeconds(30)).until(() ->
                "COMPLETED".equals(queryString("SELECT status FROM completed_routes WHERE route_id = '" + route + "'")));

        assertEquals(4, queryLong("SELECT count(*) FROM delivery_logs WHERE route_id = '" + route + "'"),
                "two arrivals + two outcomes, not doubled by the replay");
        assertEquals(1, queryLong("SELECT count(*) FROM sla_breaches WHERE route_id = '" + route + "'"));
        assertEquals(30, queryLong("SELECT breach_seconds FROM sla_breaches WHERE route_id = '" + route + "'"));
        assertEquals(1, queryLong("SELECT completed_stops FROM completed_routes WHERE route_id = '" + route + "'"));
        assertEquals(1, queryLong("SELECT failed_stops FROM completed_routes WHERE route_id = '" + route + "'"));
        assertEquals(2, queryLong("SELECT count(*) FROM delivery_logs WHERE route_id = '" + route
                + "' AND delivery_status = 'IN_PROGRESS' AND actual_departure IS NOT NULL"));
    }

    @Test
    void poisonRecordsAreDeadLetteredWithoutBlockingThePartition() throws Exception {
        String van = "van-it-poison";
        Instant t = Instant.now();
        // route_id longer than the column allows: every write attempt fails
        String tooLong = "route-" + "x".repeat(120);
        send("delivery-events", van, delivery(van, tooLong, 0, 1, DeliveryEventType.ARRIVAL, t, t.plusSeconds(60)));
        send("delivery-events", van, "{\"event_type\":\"ARRIVAL\"");
        // Same key, so same partition, queued behind the poison records
        String route = "route-it-after-poison";
        RecordMetadata good = send("delivery-events", van,
                delivery(van, route, 0, 1, DeliveryEventType.ARRIVAL, t, t.plusSeconds(60)));

        await().atMost(Duration.ofSeconds(40)).until(() ->
                queryLong("SELECT count(*) FROM delivery_logs WHERE route_id = '" + route + "'") == 1);
        assertEquals(1, queryLong("SELECT count(*) FROM dead_letter_log WHERE van_id = '" + van
                + "' AND error_reason LIKE 'DB_WRITE_FAILED%'"));
        assertEquals(1, queryLong("SELECT count(*) FROM dead_letter_log WHERE original_topic = 'delivery-events'"
                + " AND error_reason LIKE 'MALFORMED%'"));
        TopicPartition tp = new TopicPartition(good.topic(), good.partition());
        await().atMost(Duration.ofSeconds(20)).until(() ->
                committedOffset("milkrun-engine-delivery", tp) > good.offset());
    }
}
