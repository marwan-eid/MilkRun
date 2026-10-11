import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { Batcher } from '../src/batcher.js';

describe('Batcher', () => {
    let flushed: number[][];
    const flush = async (items: number[]) => { flushed.push(items); };

    /** A flush that stays in flight until released, like a request to a slow broker. */
    function slowFlush() {
        const releases: Array<() => void> = [];
        let inFlight = 0;
        let maxInFlight = 0;
        const fn = (items: number[]) => new Promise<void>((resolve) => {
            flushed.push(items);
            inFlight++;
            maxInFlight = Math.max(maxInFlight, inFlight);
            releases.push(() => { inFlight--; resolve(); });
        });
        return {
            fn,
            /** Completes the oldest flush still in flight. */
            release: async () => { releases.shift()!(); await vi.advanceTimersByTimeAsync(0); },
            get maxInFlight() { return maxInFlight; },
            get inFlight() { return inFlight; },
        };
    }

    beforeEach(() => {
        vi.useFakeTimers();
        flushed = [];
    });
    afterEach(() => {
        vi.useRealTimers();
    });

    it('sends everything added within the linger time as one batch, in order', async () => {
        const b = new Batcher(flush, 100);
        const first = b.add([1, 2]);
        b.add([3]);
        await vi.advanceTimersByTimeAsync(99);
        expect(flushed).toEqual([]);
        await vi.advanceTimersByTimeAsync(1);
        await first;
        expect(flushed).toEqual([[1, 2, 3]]);
    });

    it('flushes early once the batch is full', async () => {
        const b = new Batcher(flush, 100, 3);
        await b.add([1, 2, 3]);
        expect(flushed).toEqual([[1, 2, 3]]);
    });

    it('starts a new batch after each flush', async () => {
        const b = new Batcher(flush, 100);
        b.add([1]);
        await vi.advanceTimersByTimeAsync(100);
        b.add([2]);
        await vi.advanceTimersByTimeAsync(100);
        expect(flushed).toEqual([[1], [2]]);
    });

    it('resolves callers even when the flush fails', async () => {
        const b = new Batcher(async () => { throw new Error('broker down'); }, 10);
        const done = b.add([1]);
        await vi.advanceTimersByTimeAsync(10);
        await expect(done).resolves.toBeUndefined();
    });

    it('keeps going after a failed flush', async () => {
        let fail = true;
        const b = new Batcher(async (items: number[]) => {
            if (fail) throw new Error('broker down');
            flushed.push(items);
        }, 10);
        b.add([1]);
        await vi.advanceTimersByTimeAsync(10);
        fail = false;
        b.add([2]);
        await vi.advanceTimersByTimeAsync(10);
        expect(flushed).toEqual([[2]]);
    });

    it('drain sends immediately and ignores empty adds', async () => {
        const b = new Batcher(flush, 1000);
        await b.add([]);
        b.add([7]);
        await b.drain();
        expect(flushed).toEqual([[7]]);
    });

    it('never runs two flushes at once: what arrives meanwhile goes in the next batch', async () => {
        const slow = slowFlush();
        const b = new Batcher(slow.fn, 100);
        b.add([1]);
        await vi.advanceTimersByTimeAsync(100); // first flush is now in flight
        b.add([2]);
        await vi.advanceTimersByTimeAsync(500); // five linger times pass; the broker is still busy
        b.add([3]);
        await vi.advanceTimersByTimeAsync(500);
        expect(flushed).toEqual([[1]]);
        expect(slow.inFlight).toBe(1);

        await slow.release();
        await vi.advanceTimersByTimeAsync(100);
        expect(flushed).toEqual([[1], [2, 3]]);
        await slow.release();
        expect(slow.maxInFlight).toBe(1);
    });

    it('works through a backlog in full batches, back to back', async () => {
        const slow = slowFlush();
        const b = new Batcher(slow.fn, 100, 3);
        b.add([1, 2, 3]); // full: flushes at once
        b.add([4, 5, 6, 7, 8]);
        expect(flushed).toEqual([[1, 2, 3]]);
        await slow.release(); // next full batch starts without lingering
        expect(flushed).toEqual([[1, 2, 3], [4, 5, 6]]);
        await slow.release(); // what is left is less than a batch, so it lingers
        expect(flushed).toEqual([[1, 2, 3], [4, 5, 6]]);
        await vi.advanceTimersByTimeAsync(100);
        expect(flushed).toEqual([[1, 2, 3], [4, 5, 6], [7, 8]]);
        expect(slow.maxInFlight).toBe(1);
    });

    it('resolves each add once its own items have been flushed', async () => {
        const slow = slowFlush();
        const b = new Batcher(slow.fn, 100, 2);
        const settled: string[] = [];
        void b.add([1, 2]).then(() => settled.push('a'));
        void b.add([3, 4, 5]).then(() => settled.push('b')); // spans two batches
        await slow.release();
        expect(settled).toEqual(['a']);
        await slow.release(); // [3, 4] done, 5 still waiting
        expect(settled).toEqual(['a']);
        await vi.advanceTimersByTimeAsync(100);
        await slow.release();
        expect(flushed).toEqual([[1, 2], [3, 4], [5]]);
        expect(settled).toEqual(['a', 'b']);
    });

    it('drops the oldest waiting items when the backlog is full, and says so', async () => {
        const slow = slowFlush();
        let dropped = 0;
        const b = new Batcher(slow.fn, 100, 2, 4, (n) => { dropped += n; });
        b.add([1, 2]); // in flight
        const old = b.add([3, 4]);
        b.add([5, 6]);
        expect(dropped).toBe(0);
        b.add([7, 8]); // backlog would be 6 > 4: 3 and 4 go
        expect(dropped).toBe(2);
        await expect(old).resolves.toBeUndefined();

        await slow.release();
        await slow.release();
        await slow.release();
        expect(flushed).toEqual([[1, 2], [5, 6], [7, 8]]);
    });

    it('drain waits for the flush in flight and then sends the rest', async () => {
        const slow = slowFlush();
        const b = new Batcher(slow.fn, 1000, 2);
        b.add([1, 2]); // in flight
        b.add([3]);
        let drained = false;
        void b.drain().then(() => { drained = true; });
        await vi.advanceTimersByTimeAsync(0);
        expect(drained).toBe(false);
        await slow.release(); // [1, 2] done; drain sends [3] without lingering
        expect(flushed).toEqual([[1, 2], [3]]);
        expect(drained).toBe(false);
        await slow.release();
        expect(drained).toBe(true);
    });
});
