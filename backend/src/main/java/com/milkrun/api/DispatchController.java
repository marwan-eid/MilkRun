package com.milkrun.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.milkrun.model.DispatchEvent;
import com.milkrun.model.DispatchRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;

/**
 * Accepts ad-hoc orders dropped on the map and publishes them to Kafka.
 *
 * The endpoint is public, so requests are validated against the service area
 * and rate limited per client and globally.
 */
@RestController
@RequestMapping("/api/dispatch")
public class DispatchController {

    private static final Logger log = LoggerFactory.getLogger(DispatchController.class);
    private final DispatchPublisher publisher;
    private final ObjectMapper objectMapper;
    private final DispatchRateLimiter rateLimiter;
    private final ServiceArea serviceArea;

    public DispatchController(
            DispatchPublisher publisher,
            ObjectMapper objectMapper,
            DispatchRateLimiter rateLimiter,
            @Value("${milkrun.dispatch.service-area.min-lat}") double minLat,
            @Value("${milkrun.dispatch.service-area.max-lat}") double maxLat,
            @Value("${milkrun.dispatch.service-area.min-lon}") double minLon,
            @Value("${milkrun.dispatch.service-area.max-lon}") double maxLon) {
        this.publisher = publisher;
        this.objectMapper = objectMapper;
        this.rateLimiter = rateLimiter;
        this.serviceArea = new ServiceArea(minLat, maxLat, minLon, maxLon);
    }

    @PostMapping
    public Mono<ResponseEntity<Map<String, Object>>> dispatchOrder(
            @RequestBody DispatchRequest request, ServerHttpRequest httpRequest) {

        if (request == null || !serviceArea.contains(request.latitude(), request.longitude())) {
            return Mono.just(ResponseEntity.badRequest()
                    .body(Map.of("error", "Location is outside the service area")));
        }
        String client = clientKey(httpRequest);
        if (!rateLimiter.tryAcquire(client)) {
            return Mono.just(ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .header("Retry-After", "10")
                    .body(Map.of("error", "Too many dispatches, try again shortly")));
        }

        DispatchEvent event = new DispatchEvent(request.latitude(), request.longitude());
        String payload;
        try {
            payload = objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            return Mono.error(e);
        }

        return Mono.fromFuture(() -> publisher.publish(event.eventId(), payload))
                .timeout(Duration.ofSeconds(5))
                // then(): the future's value is irrelevant (and may be null, which
                // Reactor treats as an empty Mono), only its completion matters.
                .then(Mono.fromSupplier(() -> {
                    log.info("Dispatch {} accepted at ({}, {})", event.eventId(), event.latitude(), event.longitude());
                    return ResponseEntity.status(HttpStatus.ACCEPTED)
                            .body(Map.<String, Object>of("order_id", event.eventId()));
                }))
                .onErrorResume(e -> {
                    log.error("Failed to publish dispatch {}: {}", event.eventId(), e.toString());
                    return Mono.just(ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                            .body(Map.of("error", "Dispatch queue unavailable")));
                });
    }

    /**
     * Identifies the caller for rate limiting. Behind Caddy and nginx the first
     * X-Forwarded-For entry is the client address (Caddy replaces any value the
     * client sent itself); without a proxy it is the socket address.
     */
    static String clientKey(ServerHttpRequest request) {
        String forwarded = request.getHeaders().getFirst("X-Forwarded-For");
        if (forwarded != null && !forwarded.isBlank()) {
            return forwarded.split(",")[0].trim();
        }
        InetSocketAddress remote = request.getRemoteAddress();
        return remote != null && remote.getAddress() != null ? remote.getAddress().getHostAddress() : "unknown";
    }

    record ServiceArea(double minLat, double maxLat, double minLon, double maxLon) {
        boolean contains(double lat, double lon) {
            return lat >= minLat && lat <= maxLat && lon >= minLon && lon <= maxLon;
        }
    }
}
