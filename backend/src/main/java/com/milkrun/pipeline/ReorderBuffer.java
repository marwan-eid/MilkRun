package com.milkrun.pipeline;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;

/**
 * Per-van out-of-order reconciliation buffer.
 *
 * GPS events arrive out of order (cellular jitter, retries, partition
 * rebalances). Each van's events wait in a priority queue ordered by device
 * timestamp until they are older than the grace window, and are then released
 * in order by {@link #drainReady}. An event older than one already released
 * for its van cannot be put in order any more: {@link #offer} rejects it as
 * late so the caller can dead-letter it.
 *
 * A van's queue is released immediately when it holds a terminal event (the
 * van has finished its route, nothing more is coming) or grows past the size
 * limit.
 *
 * @param <T> buffered item; the accessors passed to the constructor read its
 *            van, timestamp, sequence number and whether it is terminal
 */
public class ReorderBuffer<T> {

    private final Duration grace;
    private final int maxBufferSize;
    private final Function<T, String> vanId;
    private final Function<T, Instant> timestamp;
    private final Comparator<T> order;
    private final Predicate<T> terminal;
    private final ConcurrentHashMap<String, VanBuffer> vanBuffers = new ConcurrentHashMap<>();

    public ReorderBuffer(Duration grace, int maxBufferSize,
            Function<T, String> vanId, Function<T, Instant> timestamp,
            ToLongFunction<T> sequenceNumber, Predicate<T> terminal) {
        this.grace = grace;
        this.maxBufferSize = maxBufferSize;
        this.vanId = vanId;
        this.timestamp = timestamp;
        this.order = Comparator.comparing(timestamp).thenComparingLong(sequenceNumber);
        this.terminal = terminal;
    }

    /**
     * Adds an item to its van's queue.
     *
     * @return false if the item is late (older than an item already released
     *         for the same van); it is not buffered
     */
    public boolean offer(T item) {
        return vanBuffers.computeIfAbsent(vanId.apply(item), k -> new VanBuffer()).offer(item);
    }

    /**
     * Removes and returns, van by van in timestamp order, every item that is
     * older than the grace window, plus all items of vans that hold a terminal
     * item or exceed the size limit.
     */
    public List<T> drainReady(Instant now) {
        Instant cutoff = now.minus(grace);
        List<T> ready = new ArrayList<>();
        for (VanBuffer buffer : vanBuffers.values()) {
            buffer.drain(cutoff, ready);
        }
        return ready;
    }

    /** Items currently held, across all vans. */
    public int size() {
        return vanBuffers.values().stream().mapToInt(VanBuffer::size).sum();
    }

    private final class VanBuffer {
        private final PriorityQueue<T> queue = new PriorityQueue<>(order);
        private Instant lastReleased = Instant.EPOCH;
        private boolean flushAll;

        synchronized boolean offer(T item) {
            if (timestamp.apply(item).isBefore(lastReleased)) {
                return false;
            }
            queue.add(item);
            if (terminal.test(item) || queue.size() > maxBufferSize) {
                flushAll = true;
            }
            return true;
        }

        synchronized void drain(Instant cutoff, List<T> out) {
            while (!queue.isEmpty() && (flushAll || timestamp.apply(queue.peek()).isBefore(cutoff))) {
                T item = queue.poll();
                lastReleased = timestamp.apply(item);
                out.add(item);
            }
            flushAll = false;
        }

        synchronized int size() {
            return queue.size();
        }
    }
}
