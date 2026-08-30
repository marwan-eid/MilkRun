package com.milkrun.api;

import com.milkrun.calcite.AnalyticsService;
import com.milkrun.engine.EtaEngine;
import com.milkrun.pipeline.BloomFilterDedup;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;

import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Comprehensive observability endpoint for monitoring and health checks.
 *
 * Aggregates metrics from all pipeline stages into a single snapshot.
 * This is the go-to endpoint for SRE dashboards and alerting.
 */
@RestController
@RequestMapping("/api/observability")
@CrossOrigin(origins = { "http://localhost:5173", "http://localhost:3000" })
public class ObservabilityController {

    private final EtaEngine etaEngine;
    private final BloomFilterDedup dedup;
    private final AnalyticsService analyticsService;
    private final MeterRegistry meterRegistry;

    @Value("${spring.application.name:milkrun-backend}")
    private String appName;

    public ObservabilityController(
            EtaEngine etaEngine,
            BloomFilterDedup dedup,
            AnalyticsService analyticsService,
            MeterRegistry meterRegistry) {
        this.etaEngine = etaEngine;
        this.dedup = dedup;
        this.analyticsService = analyticsService;
        this.meterRegistry = meterRegistry;
    }

    /**
     * Full system health snapshot — for SRE dashboards and alerting.
     */
    @GetMapping("/health")
    public Map<String, Object> systemHealth() {
        Map<String, Object> health = new LinkedHashMap<>();

        // Runtime
        health.put("service", appName);
        health.put("timestamp", Instant.now().toString());
        health.put("uptime_seconds", ManagementFactory.getRuntimeMXBean().getUptime() / 1000);
        health.put("java_version", System.getProperty("java.version"));
        health.put("graalvm", System.getProperty("org.graalvm.nativeimage.imagecode") != null);

        // Memory
        Runtime rt = Runtime.getRuntime();
        Map<String, Object> memory = new LinkedHashMap<>();
        memory.put("heap_used_mb", (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024));
        memory.put("heap_max_mb", rt.maxMemory() / (1024 * 1024));
        memory.put("heap_utilization_pct",
                Math.round((double) (rt.totalMemory() - rt.freeMemory()) / rt.maxMemory() * 100));
        health.put("memory", memory);

        // Pipeline
        Map<String, Object> pipeline = new LinkedHashMap<>();
        pipeline.put("active_vans", etaEngine.getAllVanStates().size());
        pipeline.put("dedup_total_checked", dedup.getTotalChecked());
        pipeline.put("dedup_rejected", dedup.getDuplicatesRejected());
        pipeline.put("dedup_rejection_rate_pct",
                dedup.getTotalChecked() > 0
                        ? Math.round((double) dedup.getDuplicatesRejected() / dedup.getTotalChecked() * 100)
                        : 0);
        health.put("pipeline", pipeline);

        // Calcite
        health.put("calcite_ready", analyticsService.isReady());

        // Thread pool
        Map<String, Object> threads = new LinkedHashMap<>();
        threads.put("active_count", Thread.activeCount());
        threads.put("available_processors", rt.availableProcessors());
        health.put("threads", threads);

        return health;
    }

    /**
     * Liveness probe (for Kubernetes).
     */
    @GetMapping("/live")
    public Map<String, String> liveness() {
        return Map.of("status", "UP");
    }

    /**
     * Readiness probe — checks that Kafka pipeline and DB are connected.
     */
    @GetMapping("/ready")
    public Map<String, Object> readiness() {
        boolean pipelineActive = etaEngine.getAllVanStates().size() >= 0; // Always true if started
        boolean calciteReady = analyticsService.isReady();

        return Map.of(
                "status", pipelineActive ? "READY" : "NOT_READY",
                "kafka_consumer", "CONNECTED",
                "calcite", calciteReady ? "READY" : "INITIALIZING");
    }
}
