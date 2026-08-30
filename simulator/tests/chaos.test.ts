import { describe, it, expect } from 'vitest';
import { maybeDuplicate } from '../src/chaos/duplicator.js';
import { EventReorderer } from '../src/chaos/reorderer.js';
import { ConnectionDropper } from '../src/chaos/dropper.js';
import { type GpsEvent } from '../src/models/index.js';

/** Helper: create a minimal GPS event for testing */
function makeEvent(seqNum: number, vanId = 'van-001'): GpsEvent {
    return {
        event_id: `test-${seqNum}`,
        van_id: vanId,
        sequence_number: seqNum,
        device_timestamp: new Date().toISOString(),
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

describe('Chaos Modules', () => {
    describe('Duplicator', () => {
        it('should always include the original event', () => {
            const event = makeEvent(1);
            const result = maybeDuplicate(event, { duplicateProbability: 1 });
            expect(result.length).toBeGreaterThanOrEqual(1);
            expect(result[0]).toEqual(event);
        });

        it('should create duplicates with same sequence_number but different event_id', () => {
            const event = makeEvent(42);
            const result = maybeDuplicate(event, { duplicateProbability: 1, maxDuplicates: 1 });
            expect(result.length).toBe(2);
            expect(result[1].sequence_number).toBe(42);
            expect(result[1].event_id).not.toBe(event.event_id);
        });

        it('should respect probability = 0', () => {
            const event = makeEvent(1);
            for (let i = 0; i < 100; i++) {
                const result = maybeDuplicate(event, { duplicateProbability: 0 });
                expect(result).toHaveLength(1);
            }
        });
    });

    describe('EventReorderer', () => {
        it('should buffer events until bufferSize is reached', () => {
            const reorderer = new EventReorderer({ bufferSize: 3, reorderProbability: 0 });

            expect(reorderer.push(makeEvent(1))).toHaveLength(0);
            expect(reorderer.push(makeEvent(2))).toHaveLength(0);

            const flushed = reorderer.push(makeEvent(3));
            expect(flushed).toHaveLength(3);
        });

        it('should preserve all events through the buffer', () => {
            const reorderer = new EventReorderer({ bufferSize: 5, reorderProbability: 1 });
            const events: GpsEvent[] = [];

            for (let i = 0; i < 5; i++) {
                const flushed = reorderer.push(makeEvent(i));
                events.push(...flushed);
            }
            events.push(...reorderer.flush());

            const seqNums = events.map((e) => e.sequence_number).sort();
            expect(seqNums).toEqual([0, 1, 2, 3, 4]);
        });

        it('should flush remaining events on demand', () => {
            const reorderer = new EventReorderer({ bufferSize: 10 });
            reorderer.push(makeEvent(1));
            reorderer.push(makeEvent(2));
            expect(reorderer.pending).toBe(2);

            const flushed = reorderer.flush();
            expect(flushed).toHaveLength(2);
            expect(reorderer.pending).toBe(0);
        });
    });

    describe('ConnectionDropper', () => {
        it('should drop events when drop probability is 1', () => {
            const dropper = new ConnectionDropper({
                dropProbability: 1,
                minDropTicks: 2,
                maxDropTicks: 3,
            });

            // First call triggers drop period
            expect(dropper.shouldDrop()).toBe(true);
            expect(dropper.isDropping).toBe(true);
        });

        it('should never drop when probability is 0', () => {
            const dropper = new ConnectionDropper({ dropProbability: 0 });

            for (let i = 0; i < 100; i++) {
                expect(dropper.shouldDrop()).toBe(false);
            }
            expect(dropper.totalDropped).toBe(0);
        });

        it('should track total dropped events', () => {
            const dropper = new ConnectionDropper({
                dropProbability: 1,
                minDropTicks: 5,
                maxDropTicks: 5,
            });

            for (let i = 0; i < 10; i++) {
                dropper.shouldDrop();
            }

            expect(dropper.totalDropped).toBeGreaterThan(0);
        });
    });
});
