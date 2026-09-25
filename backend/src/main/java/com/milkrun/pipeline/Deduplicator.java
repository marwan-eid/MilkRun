package com.milkrun.pipeline;

/**
 * Detects GPS events that were already seen, keyed by (van, sequence number).
 *
 * Two implementations, selected with milkrun.pipeline.dedup-strategy:
 * <ul>
 *   <li>{@code window} (default): {@link SlidingWindowDedup}, exact within a
 *       window of recent sequence numbers per van;</li>
 *   <li>{@code bloom}: {@link BloomFilterDedup}, probabilistic with a tunable
 *       false-positive rate.</li>
 * </ul>
 */
public interface Deduplicator {

    /** @return true if the event should be rejected as a duplicate. */
    boolean isDuplicate(String vanId, long sequenceNumber);

    long getTotalChecked();

    long getDuplicatesRejected();

    /** Short name for health output, e.g. "window" or "bloom". */
    String strategy();
}
