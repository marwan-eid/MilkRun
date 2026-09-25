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
    /** Simulated km/h (the simulator runs faster than real time). */
    speed_kmh: number;
    heading_degrees: number;
    battery_pct: number;
    status: VanStatus;
    current_stop_index: number;
    total_stops: number;
    /** Seconds to the next stop; 0 while serving it; -1 if unknown or no next stop. */
    eta_next_stop_seconds: number;
    sla_risk: SlaRisk;
    confidence: DataConfidence;
    in_geofence: boolean;
    geofence_name: string | null;
    last_updated: string;
    /** Deadline of the next (or current) stop. */
    sla_deadline: string | null;
    /** Predicted arrival (actual arrival while delivering). */
    predicted_arrival: string | null;
    /** Deadline minus predicted arrival, seconds; negative means late. */
    sla_slack_seconds: number | null;
    eta_method: EtaMethod;
}

export type VanStatus = 'EN_ROUTE' | 'DELIVERING' | 'IDLE' | 'RETURNING' | 'RETURNED';
export type SlaRisk = 'NONE' | 'WARNING' | 'CRITICAL' | 'UNKNOWN';
export type DataConfidence = 'REAL_TIME' | 'INTERPOLATED' | 'STALE';
export type EtaMethod = 'ROUTE' | 'STRAIGHT_LINE' | 'NONE';

/** Pipeline health endpoint response */
export interface PipelineHealth {
    activeVans: number;
    dedupChecked: number;
    dedupRejected: number;
    status: string;
}
