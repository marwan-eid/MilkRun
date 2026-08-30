import { type GpsEvent } from '../models/index.js';

/**
 * Chaos module: Delays and reorders GPS events to simulate
 * out-of-order delivery from cellular networks and Kafka partitioning.
 *
 * Uses a per-van buffer that accumulates events and randomly
 * shuffles them before releasing, simulating real-world packet reordering.
 */
export interface ReordererConfig {
    /** Probability (0–1) that a batch will be shuffled. Default: 0.08 (8%) */
    reorderProbability: number;
    /** Number of events to buffer before flushing. Default: 5 */
    bufferSize: number;
    /** Max artificial delay in ms added to reordered events. Default: 2000 */
    maxDelayMs: number;
}

const DEFAULT_CONFIG: ReordererConfig = {
    reorderProbability: 0.08,
    bufferSize: 5,
    maxDelayMs: 2000,
};

/**
 * Per-van event reorder buffer.
 * Collects events and then flushes them either in-order or shuffled.
 */
export class EventReorderer {
    private buffer: GpsEvent[] = [];
    private config: ReordererConfig;

    constructor(config: Partial<ReordererConfig> = {}) {
        this.config = { ...DEFAULT_CONFIG, ...config };
    }

    /**
     * Add an event to the buffer. Returns flushed events (may be empty
     * if the buffer hasn't reached its threshold yet).
     */
    push(event: GpsEvent): GpsEvent[] {
        this.buffer.push(event);

        if (this.buffer.length >= this.config.bufferSize) {
            return this.flush();
        }

        return [];
    }

    /**
     * Force-flush remaining events (e.g., at shutdown).
     */
    flush(): GpsEvent[] {
        if (this.buffer.length === 0) return [];

        const batch = [...this.buffer];
        this.buffer = [];

        if (Math.random() < this.config.reorderProbability) {
            // Fisher-Yates shuffle
            for (let i = batch.length - 1; i > 0; i--) {
                const j = Math.floor(Math.random() * (i + 1));
                [batch[i], batch[j]] = [batch[j], batch[i]];
            }
        }

        return batch;
    }

    /** Number of events currently buffered */
    get pending(): number {
        return this.buffer.length;
    }
}
