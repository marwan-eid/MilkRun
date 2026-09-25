package com.milkrun.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Token-bucket rate limiter for the public dispatch endpoint.
 *
 * Each client gets its own bucket, and all clients share a global bucket that
 * caps the total dispatch rate, so rotating client addresses doesn't let anyone
 * flood Kafka. Idle client buckets are evicted once they would be full again.
 */
@Component
public class DispatchRateLimiter {

    private final Clock clock;
    private final double clientCapacity;
    private final double clientRefillPerMs;
    private final Bucket global;
    private final ConcurrentHashMap<String, Bucket> clients = new ConcurrentHashMap<>();
    private final long idleEvictMs;
    private volatile long lastEvictionAt;

    @Autowired
    public DispatchRateLimiter(
            @Value("${milkrun.dispatch.rate-limit.client-burst:3}") int clientBurst,
            @Value("${milkrun.dispatch.rate-limit.client-per-minute:6}") int clientPerMinute,
            @Value("${milkrun.dispatch.rate-limit.global-per-minute:60}") int globalPerMinute) {
        this(Clock.systemUTC(), clientBurst, clientPerMinute, globalPerMinute);
    }

    DispatchRateLimiter(Clock clock, int clientBurst, int clientPerMinute, int globalPerMinute) {
        this.clock = clock;
        this.clientCapacity = clientBurst;
        this.clientRefillPerMs = clientPerMinute / 60_000.0;
        this.global = new Bucket(globalPerMinute, globalPerMinute / 60_000.0, clock.millis());
        this.idleEvictMs = (long) Math.ceil(clientBurst * 60_000.0 / clientPerMinute);
        this.lastEvictionAt = clock.millis();
    }

    /**
     * @return true if the request may proceed; false if the client or the
     *         service as a whole is over its limit.
     */
    public boolean tryAcquire(String clientKey) {
        long now = clock.millis();
        evictIdle(now);
        Bucket client = clients.computeIfAbsent(clientKey, k -> new Bucket(clientCapacity, clientRefillPerMs, now));
        synchronized (client) {
            if (!client.hasToken(now)) {
                return false;
            }
            synchronized (global) {
                if (!global.hasToken(now)) {
                    return false;
                }
                global.take();
            }
            client.take();
            return true;
        }
    }

    int trackedClients() {
        return clients.size();
    }

    private void evictIdle(long now) {
        if (now - lastEvictionAt < idleEvictMs) {
            return;
        }
        lastEvictionAt = now;
        clients.entrySet().removeIf(e -> {
            synchronized (e.getValue()) {
                return now - e.getValue().updatedAt >= idleEvictMs;
            }
        });
    }

    private static final class Bucket {
        private final double capacity;
        private final double refillPerMs;
        private double tokens;
        private long updatedAt;

        Bucket(double capacity, double refillPerMs, long now) {
            this.capacity = capacity;
            this.refillPerMs = refillPerMs;
            this.tokens = capacity;
            this.updatedAt = now;
        }

        boolean hasToken(long now) {
            tokens = Math.min(capacity, tokens + (now - updatedAt) * refillPerMs);
            updatedAt = now;
            return tokens >= 1.0;
        }

        void take() {
            tokens -= 1.0;
        }
    }
}
