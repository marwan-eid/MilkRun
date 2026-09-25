import { useState, useEffect, type ReactNode } from 'react';
import { formatSlack } from '../lib/format';

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
    avg_speed: number | null;
}

interface AtRiskVan {
    van_id: string;
    sla_risk: string;
    slack_seconds: number | null;
    zone_name: string | null;
    past_breaches: number;
    past_avg_breach_seconds: number | null;
}

interface ZoneRisk {
    van_id: string;
    zone_name: string;
    sla_risk: string;
    zone_breaches: number;
    zone_avg_breach_seconds: number | null;
}

/** Rows of a query, or the reason it failed. */
type Result<T> = { rows: T[] } | { error: string };

async function load<T>(path: string): Promise<Result<T>> {
    try {
        const res = await fetch(`${API_BASE}/api/analytics/${path}`);
        if (!res.ok) return { error: `query failed (HTTP ${res.status})` };
        return { rows: await res.json() };
    } catch {
        return { error: 'backend unreachable' };
    }
}

interface AnalyticsPanelProps {
    visible: boolean;
    onClose: () => void;
}

export function AnalyticsPanel({ visible, onClose }: AnalyticsPanelProps) {
    const [delayZones, setDelayZones] = useState<Result<DelayZone>>({ rows: [] });
    const [vanPerf, setVanPerf] = useState<Result<VanPerf>>({ rows: [] });
    const [atRisk, setAtRisk] = useState<Result<AtRiskVan>>({ rows: [] });
    const [zoneRisk, setZoneRisk] = useState<Result<ZoneRisk>>({ rows: [] });
    const [loading, setLoading] = useState(false);
    const [calciteReady, setCalciteReady] = useState(false);

    useEffect(() => {
        if (!visible) return;
        let cancelled = false;
        setLoading(true);

        Promise.all([
            load<DelayZone>('delay-zones?limit=5'),
            load<VanPerf>('van-performance?limit=10'),
            load<AtRiskVan>('at-risk-vans'),
            load<ZoneRisk>('live-zone-risk'),
            fetch(`${API_BASE}/api/analytics/status`).then(r => r.json()).catch(() => ({ calciteReady: false })),
        ]).then(([zones, perf, risk, inZones, status]) => {
            if (cancelled) return;
            setDelayZones(zones);
            setVanPerf(perf);
            setAtRisk(risk);
            setZoneRisk(inZones);
            setCalciteReady(Boolean(status.calciteReady));
            setLoading(false);
        });
        return () => { cancelled = true; };
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
                        <Card title="⚠️ At-risk vans right now" desc="Live fleet state joined with each van's breach history (federated query)"
                            result={atRisk} empty="No van is at risk right now"
                            head={['Van', 'Risk', 'Slack', 'Past breaches', 'Avg past delay']}
                            row={v => [v.van_id, v.sla_risk, formatSlack(v.slack_seconds), v.past_breaches,
                                v.past_avg_breach_seconds !== null ? `${Math.round(v.past_avg_breach_seconds)}s` : '—']} />

                        <Card title="🚧 Vans in delay zones" desc="Vans inside a zone now, with that zone's breach history (federated query)"
                            result={zoneRisk} empty="No van is inside a delay zone right now"
                            head={['Van', 'Zone', 'Risk', 'Zone breaches', 'Avg delay']}
                            row={z => [z.van_id, z.zone_name, z.sla_risk, z.zone_breaches,
                                z.zone_avg_breach_seconds !== null ? `${Math.round(z.zone_avg_breach_seconds)}s` : '—']} />

                        <Card title="🔴 Top delay zones" desc="Geofence zones where the most delivery time was lost to late arrivals"
                            result={delayZones} empty="No late arrivals inside a zone yet"
                            head={['Zone', 'Type', 'Speed', 'Breaches', 'Avg delay']}
                            row={z => [z.zone_name, <span className="type-badge">{z.zone_type}</span>, `${z.speed_factor}x`,
                                z.breach_count, `${Math.round(z.avg_breach_seconds)}s`]} />

                        <Card title="🚐 Van performance" desc="Delivery success over completed routes"
                            result={vanPerf} empty="No completed routes yet"
                            head={['Van', 'Routes', 'Completed', 'Failed', 'Success %', 'Avg speed']}
                            row={v => [v.van_id, v.total_routes,
                                <span className="text-green">{v.total_completed}</span>,
                                <span className="text-red">{v.total_failed}</span>,
                                <div className="perf-bar">
                                    <div className="perf-fill" style={{ width: `${Math.min(v.success_rate_pct, 100)}%` }} />
                                    <span>{v.success_rate_pct.toFixed(1)}%</span>
                                </div>,
                                v.avg_speed !== null ? `${v.avg_speed.toFixed(1)} km/h` : '—']} />
                    </div>
                )}

                <div className="analytics-footer">
                    <span>Apache Calcite federating PostgreSQL (JDBC adapter) with the live in-memory fleet</span>
                </div>
            </div>
        </div>
    );
}

interface CardProps<T> {
    title: string;
    desc: string;
    result: Result<T>;
    empty: string;
    head: string[];
    row: (item: T) => ReactNode[];
}

function Card<T>({ title, desc, result, empty, head, row }: CardProps<T>) {
    return (
        <div className="analytics-card">
            <h3>{title}</h3>
            <p className="card-desc">{desc}</p>
            {'error' in result ? (
                <p className="no-data">Unavailable: {result.error}</p>
            ) : result.rows.length === 0 ? (
                <p className="no-data">{empty}</p>
            ) : (
                <table className="analytics-table">
                    <thead>
                        <tr>{head.map(h => <th key={h}>{h}</th>)}</tr>
                    </thead>
                    <tbody>
                        {result.rows.map((item, i) => (
                            <tr key={i}>{row(item).map((cell, j) => <td key={j}>{cell}</td>)}</tr>
                        ))}
                    </tbody>
                </table>
            )}
        </div>
    );
}
