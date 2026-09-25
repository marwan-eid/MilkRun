package com.milkrun.api;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.*;

class DispatchRateLimiterTest {

    /** Clock whose time only moves when the test says so. */
    private static final class ManualClock extends Clock {
        private long millis = 1_000_000;

        void advance(long ms) {
            millis += ms;
        }

        @Override
        public long millis() {
            return millis;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }
    }

    @Test
    void allowsBurstThenBlocksClient() {
        ManualClock clock = new ManualClock();
        DispatchRateLimiter limiter = new DispatchRateLimiter(clock, 3, 6, 1000);

        assertTrue(limiter.tryAcquire("a"));
        assertTrue(limiter.tryAcquire("a"));
        assertTrue(limiter.tryAcquire("a"));
        assertFalse(limiter.tryAcquire("a"), "fourth request inside the burst window must be rejected");
    }

    @Test
    void refillsOverTime() {
        ManualClock clock = new ManualClock();
        DispatchRateLimiter limiter = new DispatchRateLimiter(clock, 1, 6, 1000); // one token every 10 s

        assertTrue(limiter.tryAcquire("a"));
        assertFalse(limiter.tryAcquire("a"));
        clock.advance(9_000);
        assertFalse(limiter.tryAcquire("a"));
        clock.advance(1_100);
        assertTrue(limiter.tryAcquire("a"));
    }

    @Test
    void clientsAreIndependent() {
        ManualClock clock = new ManualClock();
        DispatchRateLimiter limiter = new DispatchRateLimiter(clock, 1, 6, 1000);

        assertTrue(limiter.tryAcquire("a"));
        assertFalse(limiter.tryAcquire("a"));
        assertTrue(limiter.tryAcquire("b"));
    }

    @Test
    void globalLimitCapsAllClients() {
        ManualClock clock = new ManualClock();
        DispatchRateLimiter limiter = new DispatchRateLimiter(clock, 5, 60, 2);

        assertTrue(limiter.tryAcquire("a"));
        assertTrue(limiter.tryAcquire("b"));
        assertFalse(limiter.tryAcquire("c"), "global bucket is empty");
    }

    @Test
    void rejectedByGlobalDoesNotConsumeClientToken() {
        ManualClock clock = new ManualClock();
        DispatchRateLimiter limiter = new DispatchRateLimiter(clock, 1, 6, 1);

        assertTrue(limiter.tryAcquire("a"));
        assertFalse(limiter.tryAcquire("b")); // global empty
        clock.advance(61_000); // global refilled, b never spent its token
        assertTrue(limiter.tryAcquire("b"));
    }

    @Test
    void evictsIdleClients() {
        ManualClock clock = new ManualClock();
        DispatchRateLimiter limiter = new DispatchRateLimiter(clock, 1, 6, 1000);

        limiter.tryAcquire("a");
        limiter.tryAcquire("b");
        assertEquals(2, limiter.trackedClients());
        clock.advance(10_000); // both buckets are full again (1 token, 6/min)
        limiter.tryAcquire("c");
        assertEquals(1, limiter.trackedClients());
    }
}
