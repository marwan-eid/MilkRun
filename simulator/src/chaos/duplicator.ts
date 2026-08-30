import { type GpsEvent } from '../models/index.js';
import { v4 as uuidv4 } from 'uuid';

/**
 * Chaos module: Randomly duplicates GPS events to simulate cellular retries.
 *
 * In production, mobile/IoT devices on cellular networks frequently retry
 * sends on transient failures, leading to duplicate events arriving at Kafka.
 * This module simulates that behavior.
 */
export interface DuplicatorConfig {
    /** Probability (0–1) that any given event will be duplicated. Default: 0.05 (5%) */
    duplicateProbability: number;
    /** Max number of duplicate copies. Default: 2 */
    maxDuplicates: number;
}

const DEFAULT_CONFIG: DuplicatorConfig = {
    duplicateProbability: 0.05,
    maxDuplicates: 2,
};

/**
 * Takes an event and returns an array containing the original event
 * plus 0-N duplicates (same content but fresh event_id to simulate
 * the producer retrying the send, which would produce a new record
 * with identical payload but different Kafka offset).
 *
 * NOTE: Duplicates keep the same sequence_number as the original —
 * this is exactly what happens in real cellular retries and is what
 * the backend's Bloom filter dedup must detect.
 */
export function maybeDuplicate(
    event: GpsEvent,
    config: Partial<DuplicatorConfig> = {},
): GpsEvent[] {
    const cfg = { ...DEFAULT_CONFIG, ...config };
    const results: GpsEvent[] = [event];

    if (Math.random() < cfg.duplicateProbability) {
        const numDuplicates = 1 + Math.floor(Math.random() * cfg.maxDuplicates);
        for (let i = 0; i < numDuplicates; i++) {
            results.push({
                ...event,
                event_id: uuidv4(), // different Kafka record, same payload
            });
        }
    }

    return results;
}
