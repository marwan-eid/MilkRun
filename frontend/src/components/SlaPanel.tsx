import type { VanState } from '../types/van';

interface SlaPanelProps {
    vans: Map<string, VanState>;
    selectedVanId: string | null;
    onSelectVan: (vanId: string) => void;
}

function formatEta(seconds: number): string {
    if (seconds <= 0) return 'Now';
    const m = Math.floor(seconds / 60);
    const s = seconds % 60;
    return m > 0 ? `${m}m ${s}s` : `${s}s`;
}

function timeSince(iso: string): string {
    const diff = Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
    if (diff < 5) return 'just now';
    if (diff < 60) return `${diff}s ago`;
    return `${Math.floor(diff / 60)}m ago`;
}

export function SlaPanel({ vans, selectedVanId, onSelectVan }: SlaPanelProps) {
    const vanList = Array.from(vans.values());

    // Split into risk categories
    const critical = vanList.filter(v => v.sla_risk === 'CRITICAL');
    const warning = vanList.filter(v => v.sla_risk === 'WARNING');
    const delivering = vanList.filter(v => v.status === 'DELIVERING');
    const inGeofence = vanList.filter(v => v.in_geofence);

    return (
        <div className="sla-panel">
            <div className="panel-header">
                <h2>🥛 Fleet Monitor</h2>
                <div className="fleet-summary">
                    <span className="stat">{vanList.length} vans</span>
                    <span className="stat">{delivering.length} delivering</span>
                </div>
            </div>

            {/* Critical SLA Risks */}
            {critical.length > 0 && (
                <div className="risk-section critical">
                    <h3>🔴 Critical ({critical.length})</h3>
                    {critical.map(van => (
                        <VanCard
                            key={van.van_id}
                            van={van}
                            isSelected={van.van_id === selectedVanId}
                            onClick={onSelectVan}
                        />
                    ))}
                </div>
            )}

            {/* Warning SLA Risks */}
            {warning.length > 0 && (
                <div className="risk-section warning">
                    <h3>🟡 Warning ({warning.length})</h3>
                    {warning.map(van => (
                        <VanCard
                            key={van.van_id}
                            van={van}
                            isSelected={van.van_id === selectedVanId}
                            onClick={onSelectVan}
                        />
                    ))}
                </div>
            )}

            {/* Geofence Alerts */}
            {inGeofence.length > 0 && (
                <div className="risk-section geofence">
                    <h3>⚡ In Delay Zone ({inGeofence.length})</h3>
                    {inGeofence.map(van => (
                        <VanCard
                            key={van.van_id}
                            van={van}
                            isSelected={van.van_id === selectedVanId}
                            onClick={onSelectVan}
                        />
                    ))}
                </div>
            )}

            {/* All Vans */}
            <div className="risk-section all-vans">
                <h3>All Vans</h3>
                <div className="van-list">
                    {vanList
                        .sort((a, b) => a.van_id.localeCompare(b.van_id))
                        .map(van => (
                            <VanCard
                                key={van.van_id}
                                van={van}
                                isSelected={van.van_id === selectedVanId}
                                onClick={onSelectVan}
                            />
                        ))}
                </div>
            </div>
        </div>
    );
}

interface VanCardProps {
    van: VanState;
    isSelected: boolean;
    onClick: (vanId: string) => void;
}

function VanCard({ van, isSelected, onClick }: VanCardProps) {
    const statusColors: Record<string, string> = {
        EN_ROUTE: '#3b82f6',
        DELIVERING: '#10b981',
        IDLE: '#6b7280',
        RETURNING: '#8b5cf6',
        RETURNED: '#6b7280',
    };

    return (
        <div
            className={`van-card ${isSelected ? 'selected' : ''} ${van.sla_risk.toLowerCase()}`}
            onClick={() => onClick(van.van_id)}
        >
            <div className="van-card-header">
                <span className="van-id">{van.van_id}</span>
                <span
                    className="van-status-badge"
                    style={{ backgroundColor: statusColors[van.status] || '#6b7280' }}
                >
                    {van.status.replace('_', ' ')}
                </span>
            </div>
            <div className="van-card-body">
                <div className="van-metric">
                    <span className="label">ETA</span>
                    <span className="value">{formatEta(van.eta_next_stop_seconds)}</span>
                </div>
                <div className="van-metric">
                    <span className="label">Stop</span>
                    <span className="value">{van.current_stop_index + 1}/{van.total_stops}</span>
                </div>
                <div className="van-metric">
                    <span className="label">Speed</span>
                    <span className="value">{van.speed_kmh.toFixed(0)} km/h</span>
                </div>
                <div className="van-metric">
                    <span className="label">🔋</span>
                    <span className="value">{van.battery_pct}%</span>
                </div>
            </div>
            <div className="van-card-footer">
                <span className={`confidence ${van.status === 'RETURNED' ? 'real_time' : van.confidence.toLowerCase()}`}>
                    {van.status === 'RETURNED' ? '✓ Done' : van.confidence === 'REAL_TIME' ? '● Live' : van.confidence === 'INTERPOLATED' ? '◐ Interp.' : '○ Stale'}
                </span>
                <span className="last-update">{timeSince(van.last_updated)}</span>
            </div>
        </div>
    );
}
