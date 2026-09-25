import { usePipelineHealth } from '../hooks/usePipelineHealth';
import { useNow } from '../hooks/useNow';

interface StatsBarProps {
    connected: boolean;
    vanCount: number;
    lastEvent: number;
}

const seconds = (ms: number) => `${(ms / 1000).toFixed(1)}s`;

export function StatsBar({ connected, vanCount, lastEvent }: StatsBarProps) {
    const now = useNow();
    // Keeps counting up when the stream stalls
    const lag = lastEvent > 0 ? Math.max(0, Math.floor((now - lastEvent) / 1000)) : -1;
    const health = usePipelineHealth();

    return (
        <div className="stats-bar">
            <div className="stats-left">
                <span className="app-title">🥛 The Milk-Run</span>
                <span className="app-subtitle">Live Delivery Tracker</span>
            </div>
            <div className="stats-right">
                <span className={`connection-badge ${connected ? 'connected' : 'disconnected'}`}>
                    {connected ? '● Connected' : '○ Disconnected'}
                </span>
                <span className="stat-item">
                    🚐 {vanCount} vans
                </span>
                {health?.latencyP50Ms != null && (
                    <span
                        className={`stat-item ${health.status === 'DEGRADED' ? 'stale' : ''}`}
                        title={`Device reading to map, last few minutes. p99 ${health.latencyP99Ms != null ? seconds(health.latencyP99Ms) : '—'}. Most of it is the 3 s reorder window.`}
                    >
                        ⚡ {seconds(health.latencyP50Ms)} p50
                    </span>
                )}
                {lag >= 0 && (
                    <span className={`stat-item ${lag > 5 ? 'stale' : ''}`}>
                        ⏱ {lag}s ago
                    </span>
                )}
            </div>
        </div>
    );
}
