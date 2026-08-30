/**
 * Chaos module: Simulates cellular connection loss.
 *
 * When a van enters a "dead zone" (tunnel, parking garage, rural area),
 * all GPS pings are silently dropped for a random duration. When the
 * connection restores, a burst of buffered events arrives — which the
 * backend must handle via its out-of-order reconciliation buffer.
 */
export interface DropperConfig {
    /** Probability (0–1) of entering a drop period on any tick. Default: 0.02 (2%) */
    dropProbability: number;
    /** Minimum drop duration in ticks. Default: 4 (~2 seconds at 2 Hz) */
    minDropTicks: number;
    /** Maximum drop duration in ticks. Default: 20 (~10 seconds at 2 Hz) */
    maxDropTicks: number;
}

const DEFAULT_CONFIG: DropperConfig = {
    dropProbability: 0.02,
    minDropTicks: 4,
    maxDropTicks: 20,
};

/**
 * Connection dropper: stateful per van.
 * Call `shouldDrop()` on each tick to determine if the event should be suppressed.
 */
export class ConnectionDropper {
    private config: DropperConfig;
    private dropping: boolean = false;
    private dropTicksRemaining: number = 0;
    private _totalDropped: number = 0;

    constructor(config: Partial<DropperConfig> = {}) {
        this.config = { ...DEFAULT_CONFIG, ...config };
    }

    /**
     * Returns true if the current tick's event should be dropped (connection loss).
     */
    shouldDrop(): boolean {
        if (this.dropping) {
            this.dropTicksRemaining--;
            if (this.dropTicksRemaining <= 0) {
                this.dropping = false;
            } else {
                this._totalDropped++;
                return true;
            }
        }

        // Random chance of entering a new drop period
        if (Math.random() < this.config.dropProbability) {
            this.dropping = true;
            this.dropTicksRemaining =
                this.config.minDropTicks +
                Math.floor(Math.random() * (this.config.maxDropTicks - this.config.minDropTicks));
            this._totalDropped++;
            return true;
        }

        return false;
    }

    /** Total events dropped in this dropper's lifetime */
    get totalDropped(): number {
        return this._totalDropped;
    }

    /** Whether currently in a drop period */
    get isDropping(): boolean {
        return this.dropping;
    }
}
