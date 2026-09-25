import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { maybeDuplicate } from '../src/chaos/duplicator.js';
import { DeviceLink } from '../src/chaos/device-link.js';
import { type GpsEvent } from '../src/models/index.js';

/** Helper: create a minimal GPS event for testing */
function makeEvent(seqNum: number, vanId = 'van-001'): GpsEvent {
    return {
        event_id: `test-${seqNum}`,
        van_id: vanId,
        sequence_number: seqNum,
        device_timestamp: new Date(1_790_000_000_000 + seqNum * 500).toISOString(),
        ingestion_timestamp: null,
        location: { latitude: 52.37, longitude: 4.90 },
        speed_kmh: 25,
        heading_degrees: 90,
        battery_pct: 80,
        route_id: 'route-test',
        current_stop_index: 0,
        total_stops: 10,
        status: 'EN_ROUTE',
    };
}

/** Random source that returns the given values in turn, then `rest` forever. */
function scripted(values: number[], rest: number): () => number {
    let i = 0;
    return () => (i < values.length ? values[i++] : rest);
}

const QUIET = {
    deadZoneProbability: 0,
    jitterMinMs: 100,
    jitterMaxMs: 100,
    slowProbability: 0,
    duplicates: { duplicateProbability: 0 },
};

describe('Duplicator', () => {
    it('always includes the original event', () => {
        const event = makeEvent(1);
        const result = maybeDuplicate(event, { duplicateProbability: 1 });
        expect(result.length).toBeGreaterThanOrEqual(1);
        expect(result[0]).toEqual(event);
    });

    it('creates duplicates with the same sequence_number but a different event_id', () => {
        const event = makeEvent(42);
        const result = maybeDuplicate(event, { duplicateProbability: 1, maxDuplicates: 1 });
        expect(result.length).toBe(2);
        expect(result[1].sequence_number).toBe(42);
        expect(result[1].event_id).not.toBe(event.event_id);
    });

    it('respects probability = 0', () => {
        for (let i = 0; i < 100; i++) {
            expect(maybeDuplicate(makeEvent(1), { duplicateProbability: 0 })).toHaveLength(1);
        }
    });
});

describe('DeviceLink', () => {
    let sent: GpsEvent[][];
    const send = async (events: GpsEvent[]) => { sent.push(events); };
    const seqs = () => sent.flat().map((e) => e.sequence_number);

    beforeEach(() => {
        vi.useFakeTimers();
        sent = [];
    });
    afterEach(() => {
        vi.useRealTimers();
    });

    it('delivers each ping after its jitter delay', async () => {
        const link = new DeviceLink(send, QUIET);
        link.ping(makeEvent(1));
        link.ping(makeEvent(2));
        await vi.advanceTimersByTimeAsync(99);
        expect(sent).toHaveLength(0);
        await vi.advanceTimersByTimeAsync(1);
        expect(seqs()).toEqual([1, 2]);
    });

    it('lets jitter reorder pings, but delivers every one exactly once', async () => {
        // Per ping: dead-zone check, slow-path check, delay. Ping 1 takes the slow
        // path (300 + 0.9 x 1700 = 1830 ms), ping 2 the fast one (20 + 0.5 x 280 = 160 ms).
        const random = scripted([0.9, 0.0, 0.9, 0.0], 0.5);
        const link = new DeviceLink(send, {
            ...QUIET, jitterMinMs: 20, jitterMaxMs: 300, slowProbability: 0.5, slowMaxMs: 2000,
        }, undefined, random);
        link.ping(makeEvent(1));
        link.ping(makeEvent(2));
        await vi.advanceTimersByTimeAsync(3000);
        expect(seqs()).toEqual([2, 1]);
        expect(link.stats.sent).toBe(2);
    });

    it('sends retry duplicates with the same sequence number', async () => {
        const link = new DeviceLink(send, { ...QUIET, duplicates: { duplicateProbability: 1, maxDuplicates: 1 } });
        link.ping(makeEvent(7));
        await vi.advanceTimersByTimeAsync(200);
        expect(seqs()).toEqual([7, 7]);
        expect(new Set(sent.flat().map((e) => e.event_id)).size).toBe(2);
        expect(link.stats.duplicated).toBe(1);
    });

    it('buffers pings in a dead zone and uploads them as a delayed burst', async () => {
        // First random() enters a dead zone; afterwards nothing is lost or re-enters one.
        const random = scripted([0], 0.99);
        const link = new DeviceLink(send, {
            ...QUIET, deadZoneProbability: 0.5, minDeadZonePings: 5, maxDeadZonePings: 5,
            outageLossRate: 0.5, backfillDelayMinMs: 3000, backfillDelayMaxMs: 3000,
        }, undefined, random);

        for (let s = 1; s <= 5; s++) link.ping(makeEvent(s)); // offline
        expect(link.offline).toBe(false); // the fifth ping ended the dead zone
        link.ping(makeEvent(6)); // back online: sent live
        await vi.advanceTimersByTimeAsync(200);
        expect(seqs()).toEqual([6]);

        await vi.advanceTimersByTimeAsync(3000);
        expect(sent[1].map((e) => e.sequence_number)).toEqual([1, 2, 3, 4, 5]); // one burst, in order
        expect(link.stats.backfilled).toBe(5);
    });

    it('loses some pings in a dead zone and caps the device buffer', async () => {
        // Enter a 6-ping dead zone, lose ping 1, keep the rest
        const random = scripted([0, 0.0, 0.0], 0.99);
        const link = new DeviceLink(send, {
            ...QUIET, deadZoneProbability: 0.5, minDeadZonePings: 6, maxDeadZonePings: 6,
            outageLossRate: 0.5, deviceBufferSize: 3, backfillDelayMinMs: 1000, backfillDelayMaxMs: 1000,
        }, undefined, random);

        // Ping 1: lost (random 0.0 < 0.5). Pings 2-6 are kept, but only the last 3 fit.
        for (let s = 1; s <= 6; s++) link.ping(makeEvent(s));
        await vi.advanceTimersByTimeAsync(1000);
        expect(seqs()).toEqual([4, 5, 6]);
        expect(link.stats.lostInOutage).toBe(3);
    });

    it('final ping waits for pings in flight and the backlog, then goes last', async () => {
        const link = new DeviceLink(send, { ...QUIET, jitterMinMs: 500, jitterMaxMs: 500 });
        link.ping(makeEvent(1));
        link.ping(makeEvent(2));
        const done = link.final({ ...makeEvent(3), status: 'RETURNED' });
        await vi.advanceTimersByTimeAsync(600);
        await done;
        expect(seqs()).toEqual([1, 2, 3]);
    });

    it('close delivers everything still pending', async () => {
        const random = scripted([0], 0.99);
        const link = new DeviceLink(send, {
            ...QUIET, deadZoneProbability: 0.5, minDeadZonePings: 10, maxDeadZonePings: 10,
        }, undefined, random);
        link.ping(makeEvent(1));
        link.ping(makeEvent(2));
        await link.close();
        expect(seqs()).toEqual([1, 2]);
    });
});
