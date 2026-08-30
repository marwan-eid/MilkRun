/**
 * VanState — matches the backend's VanState record exactly.
 * Received via Server-Sent Events.
 */
export interface VanState {
    van_id: string;
    route_id: string;
    location: {
        latitude: number;
        longitude: number;
    };
    speed_kmh: number;
    heading_degrees: number;
    battery_pct: number;
    status: VanStatus;
    current_stop_index: number;
    total_stops: number;
    eta_next_stop_seconds: number;
    sla_risk: SlaRisk;
    confidence: DataConfidence;
    in_geofence: boolean;
    geofence_name: string | null;
    last_updated: string;
}

export type VanStatus = 'EN_ROUTE' | 'DELIVERING' | 'IDLE' | 'RETURNING' | 'RETURNED';
export type SlaRisk = 'NONE' | 'WARNING' | 'CRITICAL';
export type DataConfidence = 'REAL_TIME' | 'INTERPOLATED' | 'STALE';

/** Pipeline health endpoint response */
export interface PipelineHealth {
    activeVans: number;
    dedupChecked: number;
    dedupRejected: number;
    status: string;
}
