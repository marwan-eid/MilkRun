package com.milkrun.pipeline;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

class SlidingWindowDedupTest {

    @Test
    void acceptsFirstOccurrenceAndRejectsTheRepeat() {
        SlidingWindowDedup dedup = new SlidingWindowDedup(4096, 1_000_000);
        assertFalse(dedup.isDuplicate("van-001", 1));
        assertTrue(dedup.isDuplicate("van-001", 1));
        assertEquals(2, dedup.getTotalChecked());
        assertEquals(1, dedup.getDuplicatesRejected());
    }

    @Test
    void neverRejectsANewNumber() {
        // A Bloom filter sized for 1,000 insertions rejects ~2-4% of new events here
        SlidingWindowDedup dedup = new SlidingWindowDedup(64, 1_000_000);
        for (long seq = 0; seq < 1_000_000; seq++) {
            assertFalse(dedup.isDuplicate("van-001", seq), "false positive at " + seq);
        }
        assertEquals(0, dedup.getDuplicatesRejected());
    }

    @Test
    void catchesEveryDuplicateInsideTheWindowEvenOutOfOrder() {
        SlidingWindowDedup dedup = new SlidingWindowDedup(256, 1_000_000);
        List<Long> seqs = new ArrayList<>();
        for (long s = 1000; s < 1200; s++) seqs.add(s);
        Collections.shuffle(seqs, new Random(42));
        for (long s : seqs) {
            assertFalse(dedup.isDuplicate("van-001", s), "first time " + s);
        }
        Collections.shuffle(seqs, new Random(7));
        for (long s : seqs) {
            assertTrue(dedup.isDuplicate("van-001", s), "repeat " + s);
        }
    }

    @Test
    void slidesForwardAndForgetsOnlyWhatLeftTheWindow() {
        SlidingWindowDedup dedup = new SlidingWindowDedup(128, 1_000_000);
        dedup.isDuplicate("van-001", 10);
        dedup.isDuplicate("van-001", 100);
        assertTrue(dedup.isDuplicate("van-001", 10), "still inside the window");
        dedup.isDuplicate("van-001", 200);
        // 10 is now 190 behind the highest (200) with a 128-wide window: too old to tell
        assertTrue(dedup.isDuplicate("van-001", 10));
        assertEquals(1, dedup.getTooOld());
        // 150 was never seen and is inside the window
        assertFalse(dedup.isDuplicate("van-001", 150));
        assertTrue(dedup.isDuplicate("van-001", 150));
    }

    @Test
    void aLargeJumpClearsTheWindow() {
        // The simulator seeds sequences with epoch millis, so a respawned van jumps far ahead
        SlidingWindowDedup dedup = new SlidingWindowDedup(128, 1_000_000_000_000L);
        for (long s = 0; s < 100; s++) dedup.isDuplicate("van-001", s);
        long respawn = 1_790_000_000_000L;
        assertFalse(dedup.isDuplicate("van-001", respawn));
        assertFalse(dedup.isDuplicate("van-001", respawn + 1));
        assertTrue(dedup.isDuplicate("van-001", respawn));
        // A number that maps to the same slot as an old one is not mistaken for it
        assertFalse(dedup.isDuplicate("van-001", respawn - 64));
    }

    @Test
    void aCounterRestartFarBelowStartsANewWindow() {
        SlidingWindowDedup dedup = new SlidingWindowDedup(128, 10_000);
        dedup.isDuplicate("van-001", 50_000);
        assertFalse(dedup.isDuplicate("van-001", 1), "device restarted its counter");
        assertFalse(dedup.isDuplicate("van-001", 2));
        assertTrue(dedup.isDuplicate("van-001", 1));
        assertEquals(1, dedup.getResets());
    }

    @Test
    void vansAreIndependent() {
        SlidingWindowDedup dedup = new SlidingWindowDedup(64, 1_000_000);
        assertFalse(dedup.isDuplicate("van-001", 5));
        assertFalse(dedup.isDuplicate("van-002", 5));
        assertTrue(dedup.isDuplicate("van-001", 5));
    }

    @Test
    void rejectsBadWindowSizes() {
        assertThrows(IllegalArgumentException.class, () -> new SlidingWindowDedup(100, 1000));
        assertThrows(IllegalArgumentException.class, () -> new SlidingWindowDedup(0, 1000));
    }

    @Test
    void isThreadSafeAcrossVans() throws Exception {
        SlidingWindowDedup dedup = new SlidingWindowDedup(4096, 1_000_000);
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Future<Integer>> results = new ArrayList<>();
            for (int t = 0; t < 8; t++) {
                results.add(pool.submit(() -> {
                    int rejected = 0;
                    // Every thread sends the same numbers for the same 10 vans: each
                    // (van, seq) must be accepted exactly once across all threads.
                    for (long s = 0; s < 20_000; s++) {
                        if (dedup.isDuplicate("van-" + (s % 10), s)) rejected++;
                    }
                    return rejected;
                }));
            }
            int rejected = 0;
            for (Future<Integer> f : results) rejected += f.get();
            assertEquals(7 * 20_000, rejected);
            assertEquals(8 * 20_000, dedup.getTotalChecked());
        } finally {
            pool.shutdown();
        }
    }
}
