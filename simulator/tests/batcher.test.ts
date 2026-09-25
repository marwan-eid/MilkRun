import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { Batcher } from '../src/batcher.js';

describe('Batcher', () => {
    let flushed: number[][];
    const flush = async (items: number[]) => { flushed.push(items); };

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

    it('drain sends immediately and ignores empty adds', async () => {
        const b = new Batcher(flush, 1000);
        await b.add([]);
        b.add([7]);
        await b.drain();
        expect(flushed).toEqual([[7]]);
    });
});
