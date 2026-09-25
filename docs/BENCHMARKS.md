# Benchmarks

## GPS deduplication: sliding window vs Bloom filter

The pipeline drops retried GPS events by (van, sequence number). Two
implementations exist; `milkrun.pipeline.dedup-strategy` selects one.

| | Sliding window (`window`, default) | Bloom filter (`bloom`) |
|---|---|---|
| Idea | Highest sequence number seen per van plus a bitmap of the last 4,096 below it (IPsec anti-replay, RFC 4303 / RFC 6479) | Two-generation Bloom filter per van, n = 100,000, p = 0.1% |
| False positives (new event rejected) | None | About 0.1–0.2% by design; unbounded before generations were added |
| Events older than the window/filter | Rejected and counted as `too old` (≈34 min back at 2 pings/s) | Forgotten after 100k–200k insertions |
| Memory per van | 512 bytes | ≈360 KB (2 × 1,437,758 bits) |
| Memory for 50 vans | ≈25 KB | ≈18 MB |
| Throughput (1 thread) | **26.8 ± 1.9 M checks/s** | **3.4 ± 0.5 M checks/s** |
| Assumes | Sequence numbers increase per van (a jump of more than a million backwards is treated as a counter reset) | Nothing about ordering |

Throughput was measured with JMH (`backend/src/test/java/com/milkrun/bench/DedupBenchmark.java`):
50 vans round-robin, increasing sequence numbers, every 15th check a retry of the
previous number (the simulator retries ~7%). 1 fork, 3 × 2 s warm-up,
5 × 2 s measurement. Intel Core i7-8750H, OpenJDK 21.0.9, WSL2.

Both are far faster than needed: the live pipeline sees about 100 events per
second. The reasons to prefer the window are that it never rejects a real event
and uses about 700 times less memory; the Bloom filter is kept because it does not
depend on sequence numbers increasing.

### Running it

```bash
cd backend
mvn -q test-compile dependency:build-classpath \
    -Dmdep.outputFile=target/test-cp.txt -Dmdep.includeScope=test
java -cp "target/test-classes:target/classes:$(cat target/test-cp.txt)" \
    org.openjdk.jmh.Main DedupBenchmark
```

## End-to-end latency

`milkrun.pipeline.latency` measures device timestamp to published van state;
`milkrun.pipeline.ingest_lag` measures device timestamp to arrival at the
backend. Both are exposed as percentiles in `/api/observability/health`
(`latency`) and as Prometheus histograms.

Most of the end-to-end latency is the reorder buffer's grace window
(`milkrun.pipeline.reorder-buffer-grace-ms`, 3 s): an event is held until it is
3 s old so that stragglers can be put in order. Latency is therefore
≈ max(ingest lag, grace) + up to one release interval (100 ms). Lowering the
grace window lowers latency but sends more late events to the dead-letter log.
