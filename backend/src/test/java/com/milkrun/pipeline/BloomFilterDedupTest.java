package com.milkrun.pipeline;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class BloomFilterDedupTest {

    @Test
    void shouldAllowFirstOccurrence() {
        BloomFilterDedup dedup = new BloomFilterDedup(10000, 0.01);
        assertFalse(dedup.isDuplicate("van-001", 1));
    }

    @Test
    void shouldRejectSecondOccurrence() {
        BloomFilterDedup dedup = new BloomFilterDedup(10000, 0.01);
        assertFalse(dedup.isDuplicate("van-001", 1));
        assertTrue(dedup.isDuplicate("van-001", 1));
    }

    @Test
    void shouldDistinguishDifferentVans() {
        BloomFilterDedup dedup = new BloomFilterDedup(10000, 0.01);
        assertFalse(dedup.isDuplicate("van-001", 1));
        assertFalse(dedup.isDuplicate("van-002", 1));
    }

    @Test
    void shouldDistinguishDifferentSequenceNumbers() {
        BloomFilterDedup dedup = new BloomFilterDedup(10000, 0.01);
        assertFalse(dedup.isDuplicate("van-001", 1));
        assertFalse(dedup.isDuplicate("van-001", 2));
    }

    @Test
    void shouldTrackMetrics() {
        BloomFilterDedup dedup = new BloomFilterDedup(10000, 0.01);
        dedup.isDuplicate("van-001", 1);
        dedup.isDuplicate("van-001", 1);
        dedup.isDuplicate("van-002", 1);

        assertEquals(3, dedup.getTotalChecked());
        assertEquals(1, dedup.getDuplicatesRejected());
    }

    @Test
    void shouldHandleHighVolume() {
        BloomFilterDedup dedup = new BloomFilterDedup(100000, 0.001);
        int falsePositives = 0;

        // Insert 10,000 unique entries
        for (int i = 0; i < 10000; i++) {
            if (dedup.isDuplicate("van-001", i)) {
                falsePositives++;
            }
        }

        // Bloom filter should have near-zero false positives at this load
        assertTrue(falsePositives < 20, "Too many false positives: " + falsePositives);

        // All should be detected as duplicates now
        for (int i = 0; i < 100; i++) {
            assertTrue(dedup.isDuplicate("van-001", i));
        }
    }

    @Test
    void shouldNotSaturateUnderSustainedLoad() {
        // Sized for 1,000 insertions; feed it 50x that. A filter that never
        // rotates ends up rejecting nearly every new event.
        BloomFilterDedup dedup = new BloomFilterDedup(1000, 0.01);
        int falsePositives = 0;
        int total = 50_000;

        for (int i = 0; i < total; i++) {
            if (dedup.isDuplicate("van-001", i)) {
                falsePositives++;
            }
        }

        // Two live generations → roughly 2x the configured 1% at worst.
        assertTrue(falsePositives < total * 0.04, "Filter saturated: " + falsePositives + " false positives");
        assertTrue(dedup.getRotations() >= 45, "Expected ~50 rotations, got " + dedup.getRotations());
    }

    @Test
    void shouldStillCatchRecentDuplicatesAcrossRotation() {
        BloomFilterDedup dedup = new BloomFilterDedup(1000, 0.01);

        // 1,500 events: seq 0..999 rotate into the previous generation,
        // seq 1000..1499 are in the current one.
        for (int i = 0; i < 1500; i++) {
            dedup.isDuplicate("van-001", i);
        }

        assertTrue(dedup.isDuplicate("van-001", 1200), "Duplicate in current generation must be caught");
        assertTrue(dedup.isDuplicate("van-001", 500), "Duplicate in previous generation must be caught");
    }
}
