/**
 * GPS Event — emitted by each van at ~2 Hz
 */
export interface GpsEvent {
    event_id: string;
    van_id: string;
    sequence_number: number;
    device_timestamp: string;       // ISO 8601
    ingestion_timestamp: string | null;
    location: {
        latitude: number;
        longitude: number;
    };
    speed_kmh: number;
    heading_degrees: number;
    battery_pct: number;
    route_id: string;
    current_stop_index: number;
    total_stops: number;
    status: VanStatus;
}

export type VanStatus = 'EN_ROUTE' | 'DELIVERING' | 'IDLE' | 'RETURNING' | 'RETURNED';

/**
 * Delivery Event — emitted at each stop
 */
export interface DeliveryEvent {
    event_id: string;
    van_id: string;
    route_id: string;
    stop_index: number;
    customer_id: string;
    event_type: DeliveryEventType;
    timestamp: string;              // ISO 8601
    location: {
        latitude: number;
        longitude: number;
    };
    parcels_delivered: number;
    delivery_duration_seconds: number;
    sla_deadline: string;           // ISO 8601
    total_stops: number;
    notes: string | null;
}

export type DeliveryEventType = 'ARRIVAL' | 'DELIVERY_COMPLETED' | 'DELIVERY_FAILED' | 'DEPARTURE';

/**
 * A single stop on a van's route
 */
export interface RouteStop {
    stop_index: number;
    customer_id: string;
    location: {
        latitude: number;
        longitude: number;
    };
    sla_deadline: string;           // ISO 8601
    parcels: number;
}

/**
 * Waypoint for GPS interpolation between stops
 */
export interface Waypoint {
    latitude: number;
    longitude: number;
}

/**
 * A complete delivery route for a van
 */
export interface VanRoute {
    route_id: string;
    van_id: string;
    stops: RouteStop[];
    waypoints: Waypoint[];          // Dense waypoints between all stops
}
