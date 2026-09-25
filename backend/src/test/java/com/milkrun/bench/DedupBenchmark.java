package com.milkrun.bench;

import com.milkrun.pipeline.BloomFilterDedup;
import com.milkrun.pipeline.Deduplicator;
import com.milkrun.pipeline.SlidingWindowDedup;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

/**
 * Throughput of the two deduplicators on the pipeline's traffic pattern:
 * 50 vans, each sending increasing sequence numbers, with ~7% of events
 * repeated (the simulator's retry rate).
 *
 * Run: see docs/BENCHMARKS.md.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class DedupBenchmark {

    private static final int VANS = 50;
    private static final String[] VAN_IDS = new String[VANS];

    static {
        for (int i = 0; i < VANS; i++) {
            VAN_IDS[i] = String.format("van-%03d", i);
        }
    }

    @Param({ "window", "bloom" })
    public String strategy;

    private Deduplicator dedup;
    private long[] nextSeq;
    private int van;
    private long counter;

    @Setup(Level.Trial)
    public void setUp() {
        dedup = "window".equals(strategy)
                ? new SlidingWindowDedup(4096, 1_000_000)
                : new BloomFilterDedup(100_000, 0.001);
        nextSeq = new long[VANS];
        for (int i = 0; i < VANS; i++) {
            nextSeq[i] = 1_790_000_000_000L + i;
        }
    }

    /** One pipeline event: usually a new number, sometimes a retry of the previous one. */
    @Benchmark
    public boolean check() {
        van = (van + 1) % VANS;
        counter++;
        long seq = (counter % 15 == 0) ? nextSeq[van] - 1 : nextSeq[van]++;
        return dedup.isDuplicate(VAN_IDS[van], seq);
    }
}
