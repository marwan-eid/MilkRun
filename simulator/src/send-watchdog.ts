/**
 * Notices when one kind of send (GPS, deliveries, route plans) has stopped
 * getting through: every attempt has failed, or one has been hanging, for
 * longer than `stallMs`.
 *
 * A producer can end up in a state it never leaves on its own while the
 * process looks perfectly alive. The simulator checks this and exits, so the
 * container is restarted with a fresh connection instead of logging errors
 * for days.
 */
export class SendWatchdog {
    private readonly streams = new Map<string, {
        /** Start time of every send still in flight, by send id. */
        inFlight: Map<number, number>;
        failures: number;
        failingSince: number | null;
    }>();
    private nextId = 0;

    constructor(
        private readonly stallMs: number,
        private readonly now: () => number = Date.now,
        /** Failures in a row before a stream can count as stalled (one failed send proves little). */
        private readonly minFailures = 3,
    ) { }

    /** Runs a send and records how it went. Rethrows its error. */
    async track<T>(stream: string, send: () => Promise<T>): Promise<T> {
        const state = this.state(stream);
        const id = this.nextId++;
        const started = this.now();
        state.inFlight.set(id, started);
        try {
            const result = await send();
            state.failures = 0;
            state.failingSince = null;
            return result;
        } catch (e) {
            state.failures++;
            state.failingSince ??= started;
            throw e;
        } finally {
            state.inFlight.delete(id);
        }
    }

    /** Streams that have not had a successful send for `stallMs` despite trying. */
    stalled(): string[] {
        const now = this.now();
        const result: string[] = [];
        for (const [stream, state] of this.streams) {
            const failing = state.failures >= this.minFailures
                && state.failingSince !== null && now - state.failingSince >= this.stallMs;
            const hanging = [...state.inFlight.values()].some((started) => now - started >= this.stallMs);
            if (failing || hanging) result.push(stream);
        }
        return result;
    }

    private state(stream: string) {
        let state = this.streams.get(stream);
        if (!state) {
            state = { inFlight: new Map(), failures: 0, failingSince: null };
            this.streams.set(stream, state);
        }
        return state;
    }
}
