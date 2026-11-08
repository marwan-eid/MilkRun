package com.milkrun.calcite;

import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** How results are served and refreshed when queries are slower than callers wait. */
class AnalyticsServiceTest {

    /**
     * Each run blocks until released and returns its run number, so a test
     * can tell which run a result came from.
     */
    static class SlowCalcite extends CalciteSchemaFactory {
        final AtomicInteger runs = new AtomicInteger();
        final Semaphore release = new Semaphore(0);
        final Semaphore finished = new Semaphore(0);

        SlowCalcite() {
            super(null, new LiveVanTable(List::of));
        }

        @Override
        public List<Map<String, Object>> executeQuery(String sql, Object... params) {
            int run = runs.incrementAndGet();
            try {
                release.tryAcquire(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            finished.release();
            return List.of(Map.of("run", run));
        }

        @Override
        public boolean isReady() {
            return true;
        }
    }

    private static Object run(AnalyticsService.Rows rows) {
        return rows.rows().get(0).get("run");
    }

    @Test
    void aResultArrivingAfterTheTimeoutIsCachedForTheNextCaller() throws Exception {
        SlowCalcite calcite = new SlowCalcite();
        AnalyticsService analytics = new AnalyticsService(calcite, Duration.ofMinutes(1), Duration.ofMillis(200));

        StepVerifier.create(analytics.getSlaSummary()).expectError(TimeoutException.class).verify(Duration.ofSeconds(5));

        calcite.release.release();
        assertTrue(calcite.finished.tryAcquire(5, TimeUnit.SECONDS));
        // The run finishes after its caller gave up; the next caller is served from the cache.
        Thread.sleep(100);
        StepVerifier.create(analytics.getSlaSummary())
                .assertNext(rows -> assertEquals(1, run(rows)))
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
        calcite.release.release();

        StepVerifier.create(analytics.getSlaSummary()).expectNextCount(1).verifyComplete();
        assertEquals(1, calcite.runs.get(), "callers joined the run that was still going");
    }

    @Test
    void aStaleResultIsServedAtOnceWhileARefreshRuns() throws Exception {
        SlowCalcite calcite = new SlowCalcite();
        AnalyticsService analytics = new AnalyticsService(calcite, Duration.ofMillis(50), Duration.ofMinutes(10),
                Duration.ofMillis(200), Duration.ZERO);
        calcite.release.release();
        StepVerifier.create(analytics.getSlaSummary()).expectNextCount(1).verifyComplete();

        Thread.sleep(100); // past the cache TTL, within max-stale
        // Served from the last result without waiting for the second run, which is still blocked
        StepVerifier.create(analytics.getSlaSummary())
                .assertNext(rows -> assertEquals(1, run(rows)))
                .expectComplete()
                .verify(Duration.ofMillis(150));
        assertEquals(2, calcite.runs.get(), "a refresh was started");

        calcite.release.release();
        assertTrue(calcite.finished.tryAcquire(2, 5, TimeUnit.SECONDS));
        Thread.sleep(20);
        StepVerifier.create(analytics.getSlaSummary())
                .assertNext(rows -> assertEquals(2, run(rows)))
                .verifyComplete();
    }

    @Test
    void aResultOlderThanMaxStaleIsNotServed() throws Exception {
        SlowCalcite calcite = new SlowCalcite();
        AnalyticsService analytics = new AnalyticsService(calcite, Duration.ofMillis(20), Duration.ofMillis(50),
                Duration.ofMillis(200), Duration.ZERO);
        calcite.release.release();
        StepVerifier.create(analytics.getSlaSummary()).expectNextCount(1).verifyComplete();

        Thread.sleep(100); // past max-stale: the caller waits for a new run, which does not finish in time
        StepVerifier.create(analytics.getSlaSummary()).expectError(TimeoutException.class).verify(Duration.ofSeconds(5));
        calcite.release.release();
    }

    @Test
    void refreshingRunsTheDashboardQueriesSoTheyAreServedWithoutWaiting() {
        SlowCalcite calcite = new SlowCalcite();
        AnalyticsService analytics = new AnalyticsService(calcite, Duration.ofMinutes(1), Duration.ofMinutes(10),
                Duration.ofMillis(200), Duration.ZERO);
        calcite.release.release(4);

        analytics.refreshDashboard().block(Duration.ofSeconds(5));
        assertEquals(4, calcite.runs.get());

        // The dashboard's four queries, with the limits its panel asks for, are now cached
        StepVerifier.create(analytics.getAtRiskVans()).expectNextCount(1).verifyComplete();
        StepVerifier.create(analytics.getLiveZoneRisk()).expectNextCount(1).verifyComplete();
        StepVerifier.create(analytics.getTopDelayZones(AnalyticsService.DASHBOARD_DELAY_ZONES)).expectNextCount(1).verifyComplete();
        StepVerifier.create(analytics.getVanPerformance(AnalyticsService.DASHBOARD_VANS)).expectNextCount(1).verifyComplete();
        assertEquals(4, calcite.runs.get(), "no caller had to run a query");
    }
}
