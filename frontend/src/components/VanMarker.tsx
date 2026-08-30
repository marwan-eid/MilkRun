import { Marker, Popup, Tooltip } from 'react-leaflet';
import L from 'leaflet';
import type { VanState, SlaRisk, VanStatus } from '../types/van';

interface VanMarkerProps {
    van: VanState;
    isSelected: boolean;
    onClick: (vanId: string) => void;
}

/** Color map for van status + SLA risk */
function getMarkerColor(status: VanStatus, slaRisk: SlaRisk): string {
    if (slaRisk === 'CRITICAL') return '#ef4444';
    if (slaRisk === 'WARNING') return '#f59e0b';
    switch (status) {
        case 'EN_ROUTE': return '#3b82f6';
        case 'DELIVERING': return '#10b981';
        case 'IDLE': return '#6b7280';
        case 'RETURNING': return '#8b5cf6';
        case 'RETURNED': return '#6b7280';
        default: return '#6b7280';
    }
}

/** Create a custom SVG van icon with rotation and color */
function createVanIcon(color: string, heading: number, isSelected: boolean, inGeofence: boolean): L.DivIcon {
    const size = isSelected ? 28 : 20;
    const ring = isSelected ? `<circle cx="14" cy="14" r="13" fill="none" stroke="white" stroke-width="2"/>` : '';
    const geofenceRing = inGeofence ? `<circle cx="14" cy="14" r="16" fill="none" stroke="${color}" stroke-width="1" stroke-dasharray="3,2" opacity="0.6"/>` : '';

    return L.divIcon({
        className: 'van-marker',
        iconSize: [size + 8, size + 8],
        iconAnchor: [(size + 8) / 2, (size + 8) / 2],
        html: `
      <svg width="${size + 8}" height="${size + 8}" viewBox="0 0 28 28"
           style="transform: rotate(${heading}deg); transition: transform 0.3s ease;">
        ${geofenceRing}
        ${ring}
        <circle cx="14" cy="14" r="${size / 2}" fill="${color}" opacity="0.9"/>
        <polygon points="14,4 18,16 14,13 10,16" fill="white" opacity="0.9"/>
      </svg>
    `,
    });
}

/** Format seconds into "Xm Ys" */
function formatEta(seconds: number): string {
    if (seconds <= 0) return 'Arrived';
    const m = Math.floor(seconds / 60);
    const s = seconds % 60;
    return m > 0 ? `${m}m ${s}s` : `${s}s`;
}

export function VanMarker({ van, isSelected, onClick }: VanMarkerProps) {
    const icon = createVanIcon(
        getMarkerColor(van.status, van.sla_risk),
        van.heading_degrees,
        isSelected,
        van.in_geofence,
    );

    return (
        <Marker
            position={[van.location.latitude, van.location.longitude]}
            icon={icon}
            eventHandlers={{ click: () => onClick(van.van_id) }}
        >
            <Tooltip
                direction="top"
                offset={[0, -14]}
                permanent={isSelected}
                className="van-tooltip"
            >
                <div style={{ fontSize: '11px', lineHeight: 1.3 }}>
                    <strong>{van.van_id}</strong>
                    <br />
                    {van.status} • ETA {formatEta(van.eta_next_stop_seconds)}
                    {van.sla_risk !== 'NONE' && (
                        <>
                            <br />
                            <span style={{ color: van.sla_risk === 'CRITICAL' ? '#ef4444' : '#f59e0b', fontWeight: 'bold' }}>
                                ⚠ SLA {van.sla_risk}
                            </span>
                        </>
                    )}
                </div>
            </Tooltip>
            <Popup>
                <div style={{ minWidth: 180 }}>
                    <h3 style={{ margin: '0 0 8px', fontSize: '14px' }}>{van.van_id}</h3>
                    <table style={{ fontSize: '12px', lineHeight: 1.5 }}>
                        <tbody>
                            <tr><td>Route</td><td>{van.route_id}</td></tr>
                            <tr><td>Status</td><td>{van.status}</td></tr>
                            <tr><td>Speed</td><td>{van.speed_kmh.toFixed(1)} km/h</td></tr>
                            <tr><td>Battery</td><td>{van.battery_pct}%</td></tr>
                            <tr><td>Stop</td><td>{van.current_stop_index + 1}/{van.total_stops}</td></tr>
                            <tr><td>ETA</td><td>{formatEta(van.eta_next_stop_seconds)}</td></tr>
                            <tr><td>SLA</td><td>{van.sla_risk}</td></tr>
                            <tr><td>Confidence</td><td>{van.confidence}</td></tr>
                            {van.in_geofence && (
                                <tr><td>Zone</td><td>⚡ {van.geofence_name}</td></tr>
                            )}
                        </tbody>
                    </table>
                </div>
            </Popup>
        </Marker>
    );
}
