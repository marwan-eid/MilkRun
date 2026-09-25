package com.milkrun.api;

import com.milkrun.calcite.AnalyticsService;
import com.milkrun.engine.EtaEngine;
import com.milkrun.observability.PipelineHealthMonitor;
import com.milkrun.pipeline.BloomFilterDedup;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Health snapshot for dashboards and external checks.
 *
 * "status" reflects the last few minutes of pipeline traffic (see
 * {@link PipelineHealthMonitor}); the lifetime counters are kept for context.
 */
@RestController
@RequestMapping("/api/observability")
public class ObservabilityController {

    private final EtaEngine etaEngine;
    private final BloomFilterDedup dedup;
    private final AnalyticsService analyticsService;
    private final PipelineHealthMonitor healthMonitor;

    @Value("${spring.application.name:milkrun-backend}")
    private String appName;

    public ObservabilityController(
            EtaEngine etaEngine,
            BloomFilterDedup dedup,
            AnalyticsService analyticsService,
            PipelineHealthMonitor healthMonitor) {
        this.etaEngine = etaEngine;
        this.dedup = dedup;
        this.analyticsService = analyticsService;
        this.healthMonitor = healthMonitor;
    }

    @GetMapping("/health")
    public Map<String, Object> systemHealth() {
        PipelineHealthMonitor.Report window = healthMonitor.report();
        Map<String, Object> health = new LinkedHashMap<>();

        health.put("service", appName);
        health.put("status", window.status().name());
        health.put("timestamp", Instant.now().toString());
        health.put("uptime_seconds", ManagementFactory.getRuntimeMXBean().getUptime() / 1000);
        health.put("java_version", System.getProperty("java.version"));
        health.put("graalvm", System.getProperty("org.graalvm.nativeimage.imagecode") != null);

        Runtime rt = Runtime.getRuntime();
        Map<String, Object> memory = new LinkedHashMap<>();
        memory.put("heap_used_mb", (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024));
        memory.put("heap_max_mb", rt.maxMemory() / (1024 * 1024));
        memory.put("heap_utilization_pct",
                Math.round((double) (rt.totalMemory() - rt.freeMemory()) / rt.maxMemory() * 100));
        health.put("memory", memory);

        // Lifetime counters since the process started
        Map<String, Object> pipeline = new LinkedHashMap<>();
        pipeline.put("active_vans", etaEngine.getAllVanStates().size());
        pipeline.put("dedup_total_checked", dedup.getTotalChecked());
        pipeline.put("dedup_rejected", dedup.getDuplicatesRejected());
        pipeline.put("dedup_rejection_rate_pct",
                dedup.getTotalChecked() > 0
                        ? Math.round((double) dedup.getDuplicatesRejected() / dedup.getTotalChecked() * 100)
                        : 0);
        health.put("pipeline", pipeline);

        // Rates over the recent window: what alerts should look at
        health.put("window", window.toMap());

        health.put("calcite_ready", analyticsService.isReady());

        Map<String, Object> threads = new LinkedHashMap<>();
        threads.put("active_count", Thread.activeCount());
        threads.put("available_processors", rt.availableProcessors());
        health.put("threads", threads);

        return health;
    }

    /** Liveness probe: the process is up and serving HTTP. */
    @GetMapping("/live")
    public Map<String, String> liveness() {
        return Map.of("status", "UP");
    }

    /** Readiness probe: 503 until the analytics layer can serve queries. */
    @GetMapping("/ready")
    public ResponseEntity<Map<String, Object>> readiness() {
        boolean calciteReady = analyticsService.isReady();
        PipelineHealthMonitor.Report window = healthMonitor.report();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", calciteReady ? "READY" : "NOT_READY");
        body.put("calcite", calciteReady ? "READY" : "INITIALIZING");
        body.put("gps_stream", window.processed() > 0 ? "RECEIVING" : "IDLE");
        body.put("pipeline_status", window.status().name());
        return ResponseEntity.status(calciteReady ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE).body(body);
    }
}
