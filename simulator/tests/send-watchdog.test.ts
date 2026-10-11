import { describe, it, expect } from 'vitest';
import { SendWatchdog } from '../src/send-watchdog.js';

describe('SendWatchdog', () => {
    const ok = async () => 'sent';
    const fail = async () => { throw new Error('out of order sequence number'); };

    function setup(stallMs = 120_000) {
        let now = 0;
        const watchdog = new SendWatchdog(stallMs, () => now);
        return {
            watchdog,
            advance: (ms: number) => { now += ms; },
            /** One failed send every `everyMs`, for `forMs`. */
            failFor: async (stream: string, forMs: number, everyMs = 1000) => {
                for (let t = 0; t < forMs; t += everyMs) {
                    await watchdog.track(stream, fail).catch(() => { });
                    now += everyMs;
                }
            },
        };
    }

    it('is quiet while sends succeed, and returns their result', async () => {
        const { watchdog, advance } = setup();
        await expect(watchdog.track('gps', ok)).resolves.toBe('sent');
        advance(600_000);
        expect(watchdog.stalled()).toEqual([]);
    });

    it('passes the error of a failed send on', async () => {
        const { watchdog } = setup();
        await expect(watchdog.track('gps', fail)).rejects.toThrow('out of order');
    });

    it('reports a stream whose sends have all failed for the stall time', async () => {
        const { watchdog, failFor } = setup();
        await failFor('gps', 119_000);
        expect(watchdog.stalled()).toEqual([]);
        await failFor('gps', 2_000);
        expect(watchdog.stalled()).toEqual(['gps']);
    });

    it('judges each stream by itself: deliveries getting through do not hide dead GPS', async () => {
        const { watchdog, advance } = setup();
        for (let i = 0; i < 130; i++) {
            await watchdog.track('gps', fail).catch(() => { });
            await watchdog.track('delivery', ok);
            advance(1000);
        }
        expect(watchdog.stalled()).toEqual(['gps']);
    });

    it('starts over after a success', async () => {
        const { watchdog, failFor } = setup();
        await failFor('gps', 100_000);
        await watchdog.track('gps', ok);
        await failFor('gps', 100_000);
        expect(watchdog.stalled()).toEqual([]);
    });

    it('does not treat one or two failures followed by silence as a stall', async () => {
        const { watchdog, advance } = setup();
        await watchdog.track('route plans', fail).catch(() => { });
        await watchdog.track('route plans', fail).catch(() => { });
        advance(600_000);
        expect(watchdog.stalled()).toEqual([]);
    });

    it('reports a send that hangs for the stall time', async () => {
        const { watchdog, advance } = setup();
        let finish!: () => void;
        const hanging = watchdog.track('gps', () => new Promise<void>((resolve) => { finish = resolve; }));
        advance(119_000);
        expect(watchdog.stalled()).toEqual([]);
        advance(1_000);
        expect(watchdog.stalled()).toEqual(['gps']);

        finish();
        await hanging;
        expect(watchdog.stalled()).toEqual([]);
    });
});
