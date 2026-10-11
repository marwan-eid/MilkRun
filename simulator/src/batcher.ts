/**
 * Collects items and hands them to `flush` in batches of at most `maxItems`,
 * at most `lingerMs` after the first one arrived. Each `add` resolves once its
 * items have been flushed (or dropped).
 *
 * Sending 50 vans' pings individually costs a Kafka request per ping; batching
 * a tenth of a second of them is far cheaper on a small machine and still well
 * inside the backend's reorder window.
 *
 * Only one flush runs at a time. While the broker is slow, what arrives waits
 * and goes out in the next batch, so a slow broker means bigger batches rather
 * than a growing pile of requests in flight. The backlog is bounded: when it
 * exceeds `maxPending` the oldest items are dropped and reported to `onDrop`.
 */
export class Batcher<T> {
    private pending: T[] = [];
    /** One entry per `add`, in order: how many of its items are still pending. */
    private waiters: Array<{ left: number; resolve: () => void }> = [];
    private timer: ReturnType<typeof setTimeout> | null = null;
    private running: Promise<void> | null = null;
    private draining = 0;

    constructor(
        private readonly flush: (items: T[]) => Promise<void>,
        private readonly lingerMs = 100,
        private readonly maxItems = 500,
        private readonly maxPending = 20_000,
        private readonly onDrop: (dropped: number) => void = () => { },
    ) { }

    add(items: T[]): Promise<void> {
        if (items.length === 0) return Promise.resolve();
        this.pending.push(...items);
        const done = new Promise<void>((resolve) => this.waiters.push({ left: items.length, resolve }));

        const excess = this.pending.length - this.maxPending;
        if (excess > 0) {
            this.pending.splice(0, excess);
            this.settle(excess).forEach((resolve) => resolve());
            this.onDrop(excess);
        }
        this.schedule();
        return done;
    }

    /** Sends everything that is waiting, without lingering. */
    async drain(): Promise<void> {
        this.draining++;
        try {
            while (this.running || this.pending.length > 0) {
                this.schedule();
                await this.running;
            }
        } finally {
            this.draining--;
        }
    }

    /** Starts the next flush now, or after the linger time, unless one is running. */
    private schedule(): void {
        if (this.running || this.pending.length === 0) return;
        if (this.pending.length >= this.maxItems || this.draining > 0) {
            this.start();
        } else if (!this.timer) {
            this.timer = setTimeout(() => {
                this.timer = null;
                if (!this.running && this.pending.length > 0) this.start();
            }, this.lingerMs);
        }
    }

    private start(): void {
        if (this.timer) {
            clearTimeout(this.timer);
            this.timer = null;
        }
        const items = this.pending.splice(0, this.maxItems);
        const waiting = this.settle(items.length);
        this.running = (async () => {
            try {
                await this.flush(items);
            } catch {
                // The flush function reports its own errors
            } finally {
                this.running = null;
                waiting.forEach((resolve) => resolve());
                this.schedule();
            }
        })();
    }

    /** Takes the first `count` pending items off the waiters; returns those with nothing left to wait for. */
    private settle(count: number): Array<() => void> {
        const done: Array<() => void> = [];
        while (count > 0 && this.waiters.length > 0) {
            const waiter = this.waiters[0];
            const taken = Math.min(count, waiter.left);
            waiter.left -= taken;
            count -= taken;
            if (waiter.left === 0) {
                done.push(waiter.resolve);
                this.waiters.shift();
            }
        }
        return done;
    }
}
