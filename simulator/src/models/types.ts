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
    /** Speed in simulated km/h (see RoutePlan.time_scale). */
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
    /** Time spent at the stop, in simulated seconds. */
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
    /** Latest acceptable arrival (the customer's delivery slot ends here). */
    sla_deadline: string;           // ISO 8601
    /** When the planner expected the van to arrive. */
    planned_arrival: string;        // ISO 8601
    parcels: number;
    /** Index of the route polyline vertex where the van stops for this delivery. */
    waypoint_index: number;
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
    created_at: string;             // ISO 8601
    stops: RouteStop[];
    waypoints: Waypoint[];          // Street polyline: hub -> stops -> hub
}

/**
 * Published to the compacted route-plans topic (key: van_id) whenever a route
 * starts or changes, so the backend can compute ETAs along the planned path.
 */
export interface RoutePlanMessage {
    route_id: string;
    van_id: string;
    /** Increases every time this route changes (e.g. an ad-hoc stop is inserted). */
    version: number;
    created_at: string;
    /** Simulated seconds per wall-clock second. Speeds are in simulated km/h. */
    time_scale: number;
    base_speed_kmh: number;
    stops: RouteStop[];
    /** Route polyline as [latitude, longitude] pairs. */
    waypoints: [number, number][];
}
