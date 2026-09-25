/**
 * Collects items and hands them to `flush` in one call, at most `lingerMs`
 * after the first item arrived (or as soon as `maxItems` are waiting). Each
 * `add` resolves once the batch containing its items has been flushed.
 *
 * Sending 50 vans' pings individually costs a Kafka request per ping; batching
 * a tenth of a second of them is far cheaper on a small machine and still well
 * inside the backend's reorder window.
 */
export class Batcher<T> {
    private pending: T[] = [];
    private waiters: Array<() => void> = [];
    private timer: ReturnType<typeof setTimeout> | null = null;

    constructor(
        private readonly flush: (items: T[]) => Promise<void>,
        private readonly lingerMs = 100,
        private readonly maxItems = 500,
    ) { }

    add(items: T[]): Promise<void> {
        if (items.length === 0) return Promise.resolve();
        this.pending.push(...items);
        const done = new Promise<void>((resolve) => this.waiters.push(resolve));
        if (this.pending.length >= this.maxItems) {
            void this.drain();
        } else if (!this.timer) {
            this.timer = setTimeout(() => void this.drain(), this.lingerMs);
        }
        return done;
    }

    /** Sends whatever is waiting now. */
    async drain(): Promise<void> {
        if (this.timer) {
            clearTimeout(this.timer);
            this.timer = null;
        }
        if (this.pending.length === 0) return;
        const items = this.pending;
        const waiters = this.waiters;
        this.pending = [];
        this.waiters = [];
        try {
            await this.flush(items);
        } catch {
            // The flush function reports its own errors
        } finally {
            waiters.forEach((resolve) => resolve());
        }
    }
}
