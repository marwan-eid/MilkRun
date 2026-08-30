package com.milkrun.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.BitSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-van Bloom filter for duplicate GPS event detection.
 *
 * Uses (van_id, sequence_number) as the dedup key. When the simulator's
 * chaos duplicator resends events with the same sequence_number, this
 * filter catches them before they enter the reorder buffer.
 *
 * Implementation: Simple hash-based Bloom filter per van using BitSet.
 * In production, you'd use Guava's BloomFilter or a Redis-backed solution.
 */
@Component
public class BloomFilterDedup {

    private static final Logger log = LoggerFactory.getLogger(BloomFilterDedup.class);

    private final int expectedInsertions;
    private final int bitSetSize;
    private final int numHashFunctions;
    private final ConcurrentHashMap<String, BitSet> vanFilters = new ConcurrentHashMap<>();
    private final AtomicLong duplicatesRejected = new AtomicLong(0);
    private final AtomicLong totalChecked = new AtomicLong(0);

    public BloomFilterDedup(
            @Value("${milkrun.pipeline.dedup-expected-insertions:100000}") int expectedInsertions,
            @Value("${milkrun.pipeline.dedup-false-positive-rate:0.001}") double falsePositiveRate) {
        this.expectedInsertions = expectedInsertions;
        // Optimal bit set size: m = -n * ln(p) / (ln2)^2
        this.bitSetSize = (int) (-expectedInsertions * Math.log(falsePositiveRate) / (Math.log(2) * Math.log(2)));
        // Optimal hash count: k = (m/n) * ln2
        this.numHashFunctions = Math.max(1, (int) ((double) bitSetSize / expectedInsertions * Math.log(2)));
        log.info("BloomFilterDedup initialized: bitSetSize={}, hashFunctions={}", bitSetSize, numHashFunctions);
    }

    /**
     * Check if this event is a likely duplicate.
     *
     * @return true if the event should be REJECTED (duplicate), false if new.
     */
    public boolean isDuplicate(String vanId, long sequenceNumber) {
        totalChecked.incrementAndGet();
        BitSet filter = vanFilters.computeIfAbsent(vanId, k -> new BitSet(bitSetSize));
        String key = vanId + ":" + sequenceNumber;

        synchronized (filter) {
            boolean allSet = true;
            int[] hashes = computeHashes(key);

            for (int hash : hashes) {
                if (!filter.get(hash)) {
                    allSet = false;
                }
            }

            if (allSet) {
                duplicatesRejected.incrementAndGet();
                log.debug("Duplicate rejected: van={}, seq={}", vanId, sequenceNumber);
                return true;
            }

            // Mark as seen
            for (int hash : hashes) {
                filter.set(hash);
            }
            return false;
        }
    }

    /**
     * Compute k hash positions for the given key.
     * Uses double-hashing technique: h(i) = h1 + i*h2
     */
    private int[] computeHashes(String key) {
        int h1 = key.hashCode();
        int h2 = fnv1aHash(key);
        int[] result = new int[numHashFunctions];

        for (int i = 0; i < numHashFunctions; i++) {
            result[i] = Math.abs((h1 + i * h2) % bitSetSize);
        }
        return result;
    }

    private int fnv1aHash(String key) {
        int hash = 0x811c9dc5;
        for (int i = 0; i < key.length(); i++) {
            hash ^= key.charAt(i);
            hash *= 0x01000193;
        }
        return hash;
    }

    public long getDuplicatesRejected() {
        return duplicatesRejected.get();
    }

    public long getTotalChecked() {
        return totalChecked.get();
    }
}
