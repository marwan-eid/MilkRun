package com.milkrun.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.milkrun.dispatch.DispatchPlanner;
import com.milkrun.dispatch.DispatchPlanner.Assignment;
import com.milkrun.model.DispatchRequest;
import com.milkrun.model.Location;
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
import reactor.core.scheduler.Schedulers;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Accepts ad-hoc orders dropped on the map, decides which van takes each one
 * (see {@link DispatchPlanner}), and publishes the assignment to Kafka for that
 * van. The response tells the caller which van got the order and when it is
 * expected there.
 *
 * The endpoint is public, so requests are validated against the service area
 * and rate limited per client and globally.
 */
@RestController
@RequestMapping("/api/dispatch")
public class DispatchController {

    private static final Logger log = LoggerFactory.getLogger(DispatchController.class);

    private final DispatchPlanner planner;
    private final DispatchPublisher publisher;
    private final ObjectMapper objectMapper;
    private final DispatchRateLimiter rateLimiter;
    private final ServiceArea serviceArea;

    public DispatchController(
            DispatchPlanner planner,
            DispatchPublisher publisher,
            ObjectMapper objectMapper,
            DispatchRateLimiter rateLimiter,
            @Value("${milkrun.dispatch.service-area.min-lat}") double minLat,
            @Value("${milkrun.dispatch.service-area.max-lat}") double maxLat,
            @Value("${milkrun.dispatch.service-area.min-lon}") double minLon,
            @Value("${milkrun.dispatch.service-area.max-lon}") double maxLon) {
        this.planner = planner;
        this.publisher = publisher;
        this.objectMapper = objectMapper;
        this.rateLimiter = rateLimiter;
        this.serviceArea = new ServiceArea(minLat, maxLat, minLon, maxLon);
    }

    @PostMapping
    public Mono<ResponseEntity<Object>> dispatchOrder(@RequestBody DispatchRequest request, ServerHttpRequest httpRequest) {
        if (request == null || !serviceArea.contains(request.latitude(), request.longitude())) {
            return Mono.just(ResponseEntity.badRequest()
                    .body(Map.of("error", "Location is outside the service area")));
        }
        if (!rateLimiter.tryAcquire(clientKey(httpRequest))) {
            return Mono.just(ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .header("Retry-After", "10")
                    .body(Map.of("error", "Too many dispatches, try again shortly")));
        }

        DispatchPlanner.Order order = new DispatchPlanner.Order(UUID.randomUUID().toString(),
                new Location(request.latitude(), request.longitude()), Instant.now());

        return Mono.fromCallable(() -> planner.assign(order))
                .subscribeOn(Schedulers.parallel())
                .flatMap(choice -> choice
                        .map(this::publish)
                        .orElseGet(() -> Mono.just(ResponseEntity.status(HttpStatus.CONFLICT)
                                .body(Map.of("error", "No van can take an order right now")))));
    }

    private Mono<ResponseEntity<Object>> publish(Assignment assignment) {
        String payload;
        try {
            payload = objectMapper.writeValueAsString(assignment);
        } catch (Exception e) {
            return Mono.error(e);
        }
        return Mono.fromFuture(() -> publisher.publish(assignment.vanId(), payload))
                .timeout(Duration.ofSeconds(5))
                // then(): the future's value is irrelevant (and may be null, which
                // Reactor treats as an empty Mono), only its completion matters.
                .then(Mono.fromSupplier(() -> {
                    log.info("Order {} assigned to {} before stop {} (+{} km, on time: {})",
                            assignment.orderId(), assignment.vanId(), assignment.insertBefore(),
                            assignment.extraKm(), assignment.onTime());
                    return ResponseEntity.status(HttpStatus.ACCEPTED).<Object>body(assignment);
                }))
                .onErrorResume(e -> {
                    log.error("Failed to publish order {}: {}", assignment.orderId(), e.toString());
                    return Mono.just(ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                            .body(Map.of("error", "Dispatch queue unavailable")));
                });
    }

    /**
     * Identifies the caller for rate limiting. Behind Caddy and nginx the first
     * X-Forwarded-For entry is the client address (Caddy replaces any value the
     * client sent itself). With forward-headers-strategy=framework Spring has
     * already moved it into the remote address, as an unresolved address, and
     * removed the header; without a proxy the remote address is the socket's.
     */
    static String clientKey(ServerHttpRequest request) {
        String forwarded = request.getHeaders().getFirst("X-Forwarded-For");
        if (forwarded != null && !forwarded.isBlank()) {
            return forwarded.split(",")[0].trim();
        }
        InetSocketAddress remote = request.getRemoteAddress();
        return remote != null ? remote.getHostString() : "unknown";
    }

    record ServiceArea(double minLat, double maxLat, double minLon, double maxLon) {
        boolean contains(double lat, double lon) {
            return lat >= minLat && lat <= maxLat && lon >= minLon && lon <= maxLon;
        }
    }
}
