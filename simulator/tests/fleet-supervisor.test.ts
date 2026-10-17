import { describe, it, expect } from 'vitest';
import { FleetSupervisor, type Starter } from '../src/fleet-supervisor.js';

/** Lets pending promise callbacks run. */
const settle = () => new Promise((r) => setTimeout(r, 0));

describe('FleetSupervisor', () => {
    function setup(size: number, start: Starter, deadlineMs = 60_000) {
        let now = 0;
        const logs: string[] = [];
        const supervisor = new FleetSupervisor(size, start, deadlineMs, () => now, (m) => logs.push(m));
        return { supervisor, logs, advance: (ms: number) => { now += ms; } };
    }

    /** A starter that claims and succeeds at once, recording the slots it started. */
    function instant(started: number[]): Starter {
        return async (slot, claim) => {
            if (claim()) started.push(slot);
        };
    }

    it('fills the slots one per tick', async () => {
        const started: number[] = [];
        const { supervisor } = setup(3, instant(started));
        for (let i = 0; i < 5; i++) {
            supervisor.tick();
            await settle();
        }
        expect(started).toEqual([0, 1, 2]);
        expect(supervisor.active).toBe(3);
    });

    it('starts a van again in a slot that was vacated', async () => {
        const started: number[] = [];
        const { supervisor } = setup(2, instant(started));
        supervisor.tick(); supervisor.tick();
        await settle();
        supervisor.vacate(1);
        supervisor.tick();
        await settle();
        expect(started).toEqual([0, 1, 1]);
        expect(supervisor.active).toBe(2);
    });

    it('gives up on a start that hangs and tries again after the deadline', async () => {
        const started: number[] = [];
        const claims: Array<() => boolean> = [];
        let hang = true;
        const { supervisor, logs, advance } = setup(1, async (slot, claim) => {
            if (hang) {
                claims.push(claim);
                return new Promise<void>(() => { }); // like a route request that never settles
            }
            if (claim()) started.push(slot);
        }, 60_000);

        supervisor.tick();
        advance(59_000);
        supervisor.tick(); // still within the deadline: no second attempt
        await settle();
        expect(claims).toHaveLength(1);

        hang = false;
        advance(1_000);
        supervisor.tick();
        await settle();
        expect(started).toEqual([0]);
        expect(logs[0]).toContain('took over 60 s');

        // The abandoned attempt finishes after all: it must not start a second van.
        expect(claims[0]()).toBe(false);
        expect(supervisor.active).toBe(1);
    });

    it('tries again on the next tick after a failed start', async () => {
        const started: number[] = [];
        let fail = true;
        const { supervisor, logs } = setup(1, async (slot, claim) => {
            if (fail) throw new Error('router down');
            if (claim()) started.push(slot);
        });
        supervisor.tick();
        await settle();
        fail = false;
        supervisor.tick();
        await settle();
        expect(started).toEqual([0]);
        expect(logs[0]).toContain('router down');
    });

    it('never starts a second van in a slot whose van is on the road, however long its start takes', async () => {
        let attempts = 0;
        const { supervisor, advance } = setup(1, async (_slot, claim) => {
            attempts++;
            claim();
            return new Promise<void>(() => { }); // claimed, then hangs (e.g. publishing its plan)
        });
        supervisor.tick();
        await settle();
        advance(600_000);
        supervisor.tick();
        await settle();
        expect(attempts).toBe(1);
    });

    it('takes turns, so one slot that keeps failing does not hold up the others', async () => {
        const started: number[] = [];
        const { supervisor } = setup(3, async (slot, claim) => {
            if (slot === 0) throw new Error('bad slot');
            if (claim()) started.push(slot);
        });
        for (let i = 0; i < 4; i++) {
            supervisor.tick();
            await settle();
        }
        expect(started).toEqual([1, 2]);
    });
});
