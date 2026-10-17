/**
 * Keeps a van on the road in every slot.
 *
 * Starting a van (planning a route, publishing its plan) can fail or, worse,
 * hang. A van whose restart hangs would never drive again, and the fleet
 * would shrink one van at a time. So nothing starts vans except `tick()`,
 * which looks at every slot: an empty one gets a start attempt, and an
 * attempt that has not claimed its slot within `deadlineMs` is abandoned and
 * tried again. At most one attempt begins per tick, which also spaces out
 * requests to the public routing server.
 */
export type Starter = (slot: number, claim: () => boolean) => Promise<void>;

export class FleetSupervisor {
    private readonly slots: Array<{ active: boolean; attempt: { id: number; since: number } | null }>;
    private nextId = 0;
    private cursor = 0;

    /**
     * @param start begins a van in `slot`. Before putting the van on the road it
     *              must call `claim()`, and give up if that returns false (the
     *              attempt was abandoned and another one owns the slot).
     */
    constructor(
        size: number,
        private readonly start: Starter,
        private readonly deadlineMs = 60_000,
        private readonly now: () => number = Date.now,
        private readonly log: (message: string) => void = console.warn,
    ) {
        this.slots = Array.from({ length: size }, () => ({ active: false, attempt: null }));
    }

    /** Slots with a van on the road. */
    get active(): number {
        return this.slots.filter((s) => s.active).length;
    }

    /** Starts a van in the next empty slot, if any (round robin, so one failing slot cannot hold up the rest). */
    tick(): void {
        const now = this.now();
        let index = -1;
        for (let k = 0; k < this.slots.length; k++) {
            const i = (this.cursor + k) % this.slots.length;
            const s = this.slots[i];
            if (!s.active && (!s.attempt || now - s.attempt.since >= this.deadlineMs)) {
                index = i;
                break;
            }
        }
        if (index < 0) return;
        this.cursor = (index + 1) % this.slots.length;

        const slot = this.slots[index];
        if (slot.attempt) {
            this.log(`Van slot ${index}: starting a route took over ${Math.round((now - slot.attempt.since) / 1000)} s; trying again.`);
        }
        const id = ++this.nextId;
        slot.attempt = { id, since: now };
        const claim = () => {
            if (slot.attempt?.id !== id) return false;
            slot.attempt = null;
            slot.active = true;
            return true;
        };
        this.start(index, claim)
            .catch((e) => this.log(`Van slot ${index}: starting a route failed (${(e as Error).message}); trying again.`))
            .finally(() => {
                if (slot.attempt?.id === id) slot.attempt = null;
            });
    }

    /** The van in `slot` is off the road (route finished, or it failed to start). */
    vacate(slot: number): void {
        this.slots[slot].active = false;
    }
}
