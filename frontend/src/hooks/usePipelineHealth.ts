import { useEffect, useState } from 'react';

const API_BASE = import.meta.env.VITE_API_URL || '';

export interface PipelineHealth {
    status: 'WARMING_UP' | 'OK' | 'DEGRADED';
    /** Device reading to state published, milliseconds. */
    latencyP50Ms: number | null;
    latencyP99Ms: number | null;
    dedupRejectionPct: number | null;
}

/** Polls /api/observability/health; null until the first answer or while unreachable. */
export function usePipelineHealth(intervalMs = 10_000): PipelineHealth | null {
    const [health, setHealth] = useState<PipelineHealth | null>(null);

    useEffect(() => {
        let cancelled = false;
        const load = async () => {
            try {
                const res = await fetch(`${API_BASE}/api/observability/health`);
                if (!res.ok) throw new Error(`HTTP ${res.status}`);
                const body = await res.json();
                if (cancelled) return;
                const e2e = body.latency?.end_to_end_ms ?? {};
                setHealth({
                    status: body.status,
                    latencyP50Ms: e2e.p50 ?? null,
                    latencyP99Ms: e2e.p99 ?? null,
                    dedupRejectionPct: body.window?.dedup_rejection_rate_pct ?? null,
                });
            } catch {
                if (!cancelled) setHealth(null);
            }
        };
        void load();
        const id = setInterval(load, intervalMs);
        return () => {
            cancelled = true;
            clearInterval(id);
        };
    }, [intervalMs]);

    return health;
}
