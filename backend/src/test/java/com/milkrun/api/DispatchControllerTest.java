package com.milkrun.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.milkrun.dispatch.DispatchPlanner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

class DispatchControllerTest {

    /** Records published messages; can be switched to fail like an unreachable broker. */
    private static final class FakePublisher implements DispatchPublisher {
        final List<String> keys = new ArrayList<>();
        final List<String> payloads = new ArrayList<>();
        boolean failing;

        @Override
        public CompletableFuture<?> publish(String key, String payload) {
            if (failing) {
                return CompletableFuture.failedFuture(new RuntimeException("broker down"));
            }
            keys.add(key);
            payloads.add(payload);
            return CompletableFuture.completedFuture(null);
        }
    }

    private FakePublisher publisher;
    private boolean vanAvailable;
    private WebTestClient client;

    @BeforeEach
    void setUp() {
        publisher = new FakePublisher();
        vanAvailable = true;
        DispatchPlanner planner = order -> vanAvailable
                ? Optional.of(new DispatchPlanner.Assignment(order.orderId(), "van-007", "route-7",
                        order.location().latitude(), order.location().longitude(), 3, 19, 1.25,
                        Instant.parse("2026-09-25T10:05:00Z"), Instant.parse("2026-09-25T10:10:00Z"),
                        0, true, 42, Instant.parse("2026-09-25T10:00:00Z")))
                : Optional.empty();
        ObjectMapper mapper = new ObjectMapper().findAndRegisterModules()
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        DispatchController controller = new DispatchController(planner, publisher, mapper,
                new DispatchRateLimiter(2, 6, 100), 52.28, 52.43, 4.75, 5.02);
        client = WebTestClient.bindToController(controller).build();
    }

    @Test
    void assignsTheOrderAndPublishesItForThatVan() {
        post("203.0.113.7", 52.37, 4.89)
                .expectStatus().isAccepted()
                .expectBody()
                .jsonPath("$.van_id").isEqualTo("van-007")
                .jsonPath("$.insert_before").isEqualTo(3)
                .jsonPath("$.extra_km").isEqualTo(1.25)
                .jsonPath("$.on_time").isEqualTo(true)
                .jsonPath("$.order_id").isNotEmpty();

        assertEquals(List.of("van-007"), publisher.keys, "keyed by van, so the van's messages stay ordered");
        assertTrue(publisher.payloads.get(0).contains("\"latitude\":52.37"));
        assertTrue(publisher.payloads.get(0).contains("\"sla_deadline\":\"2026-09-25T10:10:00Z\""));
    }

    @Test
    void conflictWhenNoVanCanTakeIt() {
        vanAvailable = false;
        post("203.0.113.7", 52.37, 4.89).expectStatus().isEqualTo(409);
        assertTrue(publisher.payloads.isEmpty());
    }

    @Test
    void rejectsOrderOutsideServiceArea() {
        post("203.0.113.7", 48.85, 2.35).expectStatus().isBadRequest();
        assertTrue(publisher.payloads.isEmpty());
    }

    @Test
    void rejectsMissingCoordinates() {
        client.post().uri("/api/dispatch")
                .header("Content-Type", "application/json")
                .bodyValue("{}")
                .exchange()
                .expectStatus().isBadRequest();
        assertTrue(publisher.payloads.isEmpty());
    }

    @Test
    void rateLimitsPerClient() {
        post("198.51.100.1", 52.36, 4.90).expectStatus().isAccepted();
        post("198.51.100.1", 52.36, 4.90).expectStatus().isAccepted();
        post("198.51.100.1", 52.36, 4.90).expectStatus().isEqualTo(429).expectHeader().exists("Retry-After");
        post("198.51.100.2", 52.36, 4.90).expectStatus().isAccepted();
        assertEquals(3, publisher.payloads.size());
    }

    @Test
    void reportsUnavailableWhenKafkaFails() {
        publisher.failing = true;
        post("192.0.2.1", 52.36, 4.90).expectStatus().isEqualTo(503);
    }

    private WebTestClient.ResponseSpec post(String ip, double lat, double lon) {
        return client.post().uri("/api/dispatch")
                .header("X-Forwarded-For", ip + ", 172.18.0.5")
                .header("Content-Type", "application/json")
                .bodyValue("{\"latitude\":" + lat + ",\"longitude\":" + lon + "}")
                .exchange();
    }
}
