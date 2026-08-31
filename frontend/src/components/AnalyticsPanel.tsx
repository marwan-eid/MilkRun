import { useState, useEffect } from 'react';

const API_BASE = import.meta.env.VITE_API_URL || '';

interface DelayZone {
    zone_name: string;
    zone_type: string;
    speed_factor: number;
    breach_count: number;
    total_breach_seconds: number;
    avg_breach_seconds: number;
}

interface VanPerf {
    van_id: string;
    total_routes: number;
    total_completed: number;
    total_failed: number;
    success_rate_pct: number;
    avg_speed: number;
}

interface AnalyticsPanelProps {
    visible: boolean;
    onClose: () => void;
}

export function AnalyticsPanel({ visible, onClose }: AnalyticsPanelProps) {
    const [delayZones, setDelayZones] = useState<DelayZone[]>([]);
    const [vanPerf, setVanPerf] = useState<VanPerf[]>([]);
    const [loading, setLoading] = useState(false);
    const [calciteReady, setCalciteReady] = useState(false);

    useEffect(() => {
        if (!visible) return;

        setLoading(true);

        Promise.all([
            fetch(`${API_BASE}/api/analytics/delay-zones?limit=5`).then(r => r.json()).catch(() => []),
            fetch(`${API_BASE}/api/analytics/van-performance?limit=10`).then(r => r.json()).catch(() => []),
            fetch(`${API_BASE}/api/analytics/status`).then(r => r.json()).catch(() => ({ calciteReady: false })),
        ]).then(([zones, perf, status]) => {
            setDelayZones(zones);
            setVanPerf(perf);
            setCalciteReady(status.calciteReady);
            setLoading(false);
        });
    }, [visible]);

    if (!visible) return null;

    return (
        <div className="analytics-overlay" onClick={onClose}>
            <div className="analytics-modal" onClick={e => e.stopPropagation()}>
                <div className="analytics-header">
                    <h2>📊 Fleet Analytics</h2>
                    <div className="analytics-meta">
                        <span className={`calcite-badge ${calciteReady ? 'ready' : 'offline'}`}>
                            Calcite {calciteReady ? '● Ready' : '○ Offline'}
                        </span>
                        <button className="close-btn" onClick={onClose}>✕</button>
                    </div>
                </div>

                {loading ? (
                    <div className="analytics-loading">
                        <div className="spinner" />
                        <span>Querying via Apache Calcite…</span>
                    </div>
                ) : (
                    <div className="analytics-grid">
                        {/* Delay Zones */}
                        <div className="analytics-card">
                            <h3>🔴 Top Delay Zones</h3>
                            <p className="card-desc">Geofence zones causing the most SLA breaches</p>
                            {delayZones.length > 0 ? (
                                <table className="analytics-table">
                                    <thead>
                                        <tr>
                                            <th>Zone</th>
                                            <th>Type</th>
                                            <th>Speed Factor</th>
                                            <th>Breaches</th>
                                            <th>Avg Delay</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {delayZones.map(z => (
                                            <tr key={z.zone_name}>
                                                <td>{z.zone_name}</td>
                                                <td><span className="type-badge">{z.zone_type}</span></td>
                                                <td>{z.speed_factor}x</td>
                                                <td>{z.breach_count}</td>
                                                <td>{Math.round(z.avg_breach_seconds)}s</td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            ) : (
                                <p className="no-data">No breach data yet — start the simulator first</p>
                            )}
                        </div>

                        {/* Van Performance */}
                        <div className="analytics-card">
                            <h3>🚐 Van Performance</h3>
                            <p className="card-desc">Route completion and efficiency rankings</p>
                            {vanPerf.length > 0 ? (
                                <table className="analytics-table">
                                    <thead>
                                        <tr>
                                            <th>Van</th>
                                            <th>Routes</th>
                                            <th>Completed</th>
                                            <th>Failed</th>
                                            <th>Success %</th>
                                            <th>Avg Speed</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {vanPerf.map(v => (
                                            <tr key={v.van_id}>
                                                <td>{v.van_id}</td>
                                                <td>{v.total_routes}</td>
                                                <td className="text-green">{v.total_completed}</td>
                                                <td className="text-red">{v.total_failed}</td>
                                                <td>
                                                    <div className="perf-bar">
                                                        <div
                                                            className="perf-fill"
                                                            style={{ width: `${Math.min(v.success_rate_pct, 100)}%` }}
                                                        />
                                                        <span>{v.success_rate_pct.toFixed(1)}%</span>
                                                    </div>
                                                </td>
                                                <td>{v.avg_speed.toFixed(1)} km/h</td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            ) : (
                                <p className="no-data">No completed routes yet</p>
                            )}
                        </div>
                    </div>
                )}

                <div className="analytics-footer">
                    <span>Powered by Apache Calcite • Federated PostgreSQL JDBC adapter</span>
                </div>
            </div>
        </div>
    );
}
