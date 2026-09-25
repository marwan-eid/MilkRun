import { type GpsEvent } from '../models/index.js';
import { maybeDuplicate, type DuplicatorConfig } from './duplicator.js';

/**
 * Chaos module: the van's cellular link to Kafka.
 *
 * - Jitter: every ping is delivered after a small random delay (a few are
 *   slow), so pings from one van naturally arrive out of order.
 * - Retries: some pings are delivered two or three times (see duplicator).
 * - Dead zones (tunnels, garages): for a while nothing gets through. The
 *   device keeps recording and buffers the pings (some are lost); once
 *   connected again, live pings flow immediately and the backlog is uploaded
 *   as a burst a few seconds later. If the backlog arrives before the
 *   backend has moved past it, it is merged back in order; if it arrives
 *   after newer pings were already released, it is late and dead-lettered.
 */
export interface DeviceLinkConfig {
    /** Probability, per ping, of entering a dead zone. */
    deadZoneProbability: number;
    minDeadZonePings: number;
    maxDeadZonePings: number;
    /** Share of pings recorded in a dead zone that never make it into the backlog. */
    outageLossRate: number;
    /** Pings the device keeps while offline; the oldest are discarded first. */
    deviceBufferSize: number;
    /** Delay, after reconnecting, before the backlog is uploaded. */
    backfillDelayMinMs: number;
    backfillDelayMaxMs: number;
    /** Normal per-ping delivery delay. */
    jitterMinMs: number;
    jitterMaxMs: number;
    /** Share of pings that take a slow path, up to slowMaxMs. */
    slowProbability: number;
    slowMaxMs: number;
    duplicates: Partial<DuplicatorConfig>;
}

export const DEFAULT_LINK_CONFIG: DeviceLinkConfig = {
    deadZoneProbability: 0.005,
    minDeadZonePings: 4,
    maxDeadZonePings: 20,
    outageLossRate: 0.1,
    deviceBufferSize: 60,
    backfillDelayMinMs: 1000,
    backfillDelayMaxMs: 6000,
    jitterMinMs: 20,
    jitterMaxMs: 300,
    slowProbability: 0.05,
    slowMaxMs: 2000,
    duplicates: {},
};

export type Scheduler = (fn: () => void, delayMs: number) => void;

export interface LinkStats {
    sent: number;
    lostInOutage: number;
    backfilled: number;
    duplicated: number;
}

export class DeviceLink {
    private readonly config: DeviceLinkConfig;
    private deadZonePingsLeft = 0;
    private backlog: GpsEvent[] = [];
    private readonly inFlight = new Set<Promise<void>>();
    private readonly _stats: LinkStats = { sent: 0, lostInOutage: 0, backfilled: 0, duplicated: 0 };

    constructor(
        private readonly send: (events: GpsEvent[]) => Promise<void>,
        config: Partial<DeviceLinkConfig> = {},
        private readonly schedule: Scheduler = (fn, ms) => { setTimeout(fn, ms); },
        private readonly random: () => number = Math.random,
    ) {
        this.config = { ...DEFAULT_LINK_CONFIG, ...config };
    }

    get offline(): boolean {
        return this.deadZonePingsLeft > 0;
    }

    get stats(): LinkStats {
        return { ...this._stats };
    }

    /** Hands a ping to the link. Returns immediately; delivery happens later. */
    ping(event: GpsEvent): void {
        if (this.deadZonePingsLeft === 0 && this.random() < this.config.deadZoneProbability) {
            const span = this.config.maxDeadZonePings - this.config.minDeadZonePings;
            this.deadZonePingsLeft = this.config.minDeadZonePings + Math.floor(this.random() * (span + 1));
        }

        if (this.deadZonePingsLeft > 0) {
            this.deadZonePingsLeft--;
            if (this.random() < this.config.outageLossRate) {
                this._stats.lostInOutage++;
            } else {
                this.backlog.push(event);
                if (this.backlog.length > this.config.deviceBufferSize) {
                    this.backlog.shift();
                    this._stats.lostInOutage++;
                }
            }
            if (this.deadZonePingsLeft === 0) {
                this.scheduleBackfill();
            }
            return;
        }

        const copies = maybeDuplicate(event, this.config.duplicates);
        this._stats.duplicated += copies.length - 1;
        for (const copy of copies) {
            this.deliver([copy], this.jitter());
        }
    }

    /**
     * Sends a ping that must arrive last (the final RETURNED ping): waits for
     * everything still in flight, uploads any backlog, then sends it.
     */
    async final(event: GpsEvent): Promise<void> {
        this.deadZonePingsLeft = 0;
        await this.drain();
        if (this.backlog.length > 0) {
            const backlog = this.backlog;
            this.backlog = [];
            this._stats.backfilled += backlog.length;
            await this.sendCounted(backlog);
        }
        await this.sendCounted([event]);
    }

    /** Delivers everything pending right away (shutdown). */
    async close(): Promise<void> {
        this.deadZonePingsLeft = 0;
        await this.drain();
        if (this.backlog.length > 0) {
            const backlog = this.backlog;
            this.backlog = [];
            this._stats.backfilled += backlog.length;
            await this.sendCounted(backlog);
        }
    }

    /** Resolves once every scheduled delivery has been sent. */
    async drain(): Promise<void> {
        while (this.inFlight.size > 0) {
            await Promise.all([...this.inFlight]);
        }
    }

    private scheduleBackfill(): void {
        const backlog = this.backlog;
        this.backlog = [];
        if (backlog.length === 0) return;
        const span = this.config.backfillDelayMaxMs - this.config.backfillDelayMinMs;
        this._stats.backfilled += backlog.length;
        this.deliver(backlog, this.config.backfillDelayMinMs + this.random() * span);
    }

    private jitter(): number {
        const c = this.config;
        return this.random() < c.slowProbability
            ? c.jitterMaxMs + this.random() * (c.slowMaxMs - c.jitterMaxMs)
            : c.jitterMinMs + this.random() * (c.jitterMaxMs - c.jitterMinMs);
    }

    private deliver(events: GpsEvent[], delayMs: number): void {
        let resolve!: () => void;
        const done = new Promise<void>((r) => { resolve = r; });
        this.inFlight.add(done);
        this.schedule(() => {
            this.sendCounted(events)
                .catch(() => { /* the producer counts and logs its own errors */ })
                .finally(() => {
                    this.inFlight.delete(done);
                    resolve();
                });
        }, delayMs);
    }

    private async sendCounted(events: GpsEvent[]): Promise<void> {
        this._stats.sent += events.length;
        await this.send(events);
    }
}
