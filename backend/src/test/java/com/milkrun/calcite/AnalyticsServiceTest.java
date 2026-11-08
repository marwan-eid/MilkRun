package com.milkrun.calcite;

import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** How queries slower than the caller's timeout are run and cached. */
class AnalyticsServiceTest {

    /** A query that blocks until released, counting how often it actually runs. */
    static class SlowCalcite extends CalciteSchemaFactory {
        final AtomicInteger runs = new AtomicInteger();
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch finished = new CountDownLatch(1);

        SlowCalcite() {
            super(null, new LiveVanTable(List::of));
        }

        @Override
        public List<Map<String, Object>> executeQuery(String sql, Object... params) {
            runs.incrementAndGet();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            finished.countDown();
            return List.of(Map.of("severity", "WARNING"));
        }
    }

    @Test
    void aResultArrivingAfterTheTimeoutIsCachedForTheNextCaller() throws Exception {
        SlowCalcite calcite = new SlowCalcite();
        AnalyticsService analytics = new AnalyticsService(calcite, Duration.ofMinutes(1), Duration.ofMillis(200));

        StepVerifier.create(analytics.getSlaSummary()).expectError(TimeoutException.class).verify(Duration.ofSeconds(5));

        calcite.release.countDown();
        assertTrue(calcite.finished.await(5, TimeUnit.SECONDS));
        // The run finishes after its caller gave up; the next caller is served from the cache.
        Thread.sleep(100);
        StepVerifier.create(analytics.getSlaSummary())
                .expectNext(List.of(Map.of("severity", "WARNING")))
                .verifyComplete();
        assertEquals(1, calcite.runs.get());
    }

    @Test
    void concurrentCallersShareOneRunInsteadOfQueueingMore() {
        SlowCalcite calcite = new SlowCalcite();
        AnalyticsService analytics = new AnalyticsService(calcite, Duration.ofMinutes(1), Duration.ofMillis(200));

        for (int i = 0; i < 5; i++) {
            StepVerifier.create(analytics.getSlaSummary()).expectError(TimeoutException.class).verify(Duration.ofSeconds(5));
        }
        calcite.release.countDown();

        StepVerifier.create(analytics.getSlaSummary()).expectNextCount(1).verifyComplete();
        assertEquals(1, calcite.runs.get(), "callers joined the run that was still going");
    }
}
