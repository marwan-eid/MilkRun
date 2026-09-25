package com.milkrun.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

class DispatchControllerTest {

    /** Records published messages; can be switched to fail like an unreachable broker. */
    private static final class FakePublisher implements DispatchPublisher {
        final List<String> payloads = new ArrayList<>();
        boolean failing;

        @Override
        public CompletableFuture<?> publish(String key, String payload) {
            if (failing) {
                return CompletableFuture.failedFuture(new RuntimeException("broker down"));
            }
            payloads.add(payload);
            return CompletableFuture.completedFuture(null);
        }
    }

    private FakePublisher publisher;
    private WebTestClient client;

    @BeforeEach
    void setUp() {
        publisher = new FakePublisher();
        DispatchController controller = new DispatchController(
                publisher, new ObjectMapper().findAndRegisterModules(),
                new DispatchRateLimiter(2, 6, 100),
                52.28, 52.43, 4.75, 5.02);
        client = WebTestClient.bindToController(controller).build();
    }

    @Test
    void acceptsOrderInsideServiceArea() {
        post("203.0.113.7", 52.37, 4.89)
                .expectStatus().isAccepted()
                .expectBody().jsonPath("$.order_id").isNotEmpty();

        assertEquals(1, publisher.payloads.size());
        assertTrue(publisher.payloads.get(0).contains("52.37"));
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
