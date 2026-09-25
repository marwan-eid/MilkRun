package com.milkrun.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Exact duplicate detection with a sliding window per van, the anti-replay
 * technique IPsec uses (RFC 4303, RFC 6479).
 *
 * A van's sequence numbers only increase, so it is enough to remember the
 * highest one seen and a bitmap of which of the {@code windowSize} numbers
 * below it have arrived. A number above the highest is new; one inside the
 * window is a duplicate exactly when its bit is set. There are no false
 * positives, lookups are O(1), and memory is windowSize / 8 bytes per van.
 *
 * A number below the window is older than anything that can still be told
 * apart; it is rejected (and counted separately), like IPsec does. At two
 * pings a second, a 4096-number window covers the last ~34 minutes, far
 * longer than a retried or delayed event takes to arrive. A number more than
 * {@code resetDistance} below the highest is taken to mean the device
 * restarted its counter, and the window starts over from it.
 */
@Component
@ConditionalOnProperty(name = "milkrun.pipeline.dedup-strategy", havingValue = "window", matchIfMissing = true)
public class SlidingWindowDedup implements Deduplicator {

    private static final Logger log = LoggerFactory.getLogger(SlidingWindowDedup.class);

    private final int windowSize;
    private final long resetDistance;
    private final ConcurrentHashMap<String, Window> windows = new ConcurrentHashMap<>();
    private final AtomicLong totalChecked = new AtomicLong();
    private final AtomicLong duplicatesRejected = new AtomicLong();
    private final AtomicLong tooOld = new AtomicLong();
    private final AtomicLong resets = new AtomicLong();

    /** Per-van state; guarded by its own monitor. */
    private final class Window {
        final long[] bits = new long[windowSize / 64];
        long highest;
        boolean empty = true;

        boolean get(long seq) {
            int slot = (int) Math.floorMod(seq, (long) windowSize);
            return (bits[slot >>> 6] & (1L << slot)) != 0;
        }

        void set(long seq) {
            int slot = (int) Math.floorMod(seq, (long) windowSize);
            bits[slot >>> 6] |= 1L << slot;
        }

        void clear(long seq) {
            int slot = (int) Math.floorMod(seq, (long) windowSize);
            bits[slot >>> 6] &= ~(1L << slot);
        }

        void startAt(long seq) {
            Arrays.fill(bits, 0);
            highest = seq;
            empty = false;
            set(seq);
        }
    }

    public SlidingWindowDedup(
            @Value("${milkrun.pipeline.dedup-window-size:4096}") int windowSize,
            @Value("${milkrun.pipeline.dedup-reset-distance:1000000}") long resetDistance) {
        if (windowSize < 64 || windowSize % 64 != 0) {
            throw new IllegalArgumentException("windowSize must be a positive multiple of 64");
        }
        this.windowSize = windowSize;
        this.resetDistance = resetDistance;
        log.info("SlidingWindowDedup initialized: window={} per van ({} bytes)", windowSize, windowSize / 8);
    }

    @Override
    public boolean isDuplicate(String vanId, long seq) {
        totalChecked.incrementAndGet();
        Window w = windows.computeIfAbsent(vanId, k -> new Window());
        synchronized (w) {
            if (w.empty) {
                w.startAt(seq);
                return false;
            }
            if (seq > w.highest) {
                long advance = seq - w.highest;
                if (advance >= windowSize) {
                    Arrays.fill(w.bits, 0);
                } else {
                    // Slots between the old and new highest now belong to new numbers
                    for (long s = w.highest + 1; s < seq; s++) {
                        w.clear(s);
                    }
                }
                w.highest = seq;
                w.set(seq);
                return false;
            }
            long behind = w.highest - seq;
            if (behind >= resetDistance) {
                resets.incrementAndGet();
                w.startAt(seq);
                return false;
            }
            if (behind >= windowSize) {
                tooOld.incrementAndGet();
                duplicatesRejected.incrementAndGet();
                return true;
            }
            if (w.get(seq)) {
                duplicatesRejected.incrementAndGet();
                return true;
            }
            w.set(seq);
            return false;
        }
    }

    @Override
    public long getTotalChecked() {
        return totalChecked.get();
    }

    @Override
    public long getDuplicatesRejected() {
        return duplicatesRejected.get();
    }

    /** Rejected because they were older than the window (not provably duplicates). */
    public long getTooOld() {
        return tooOld.get();
    }

    /** Windows restarted because a van's sequence jumped far backwards. */
    public long getResets() {
        return resets.get();
    }

    @Override
    public String strategy() {
        return "window";
    }
}
