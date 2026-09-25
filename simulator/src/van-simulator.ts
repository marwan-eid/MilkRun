import { v4 as uuidv4 } from 'uuid';
import {
    type GpsEvent, type DeliveryEvent, type VanRoute, type VanStatus, type RouteStop, type Waypoint,
    type RoutePlanMessage,
} from './models/index.js';
import { calculateBearing, cumulativeDistances, lerp, speedFactorAt } from './geo.js';
import { type Router, travelSeconds } from './route-generator.js';
import { maybeDuplicate, EventReorderer, ConnectionDropper } from './chaos/index.js';

/** What the simulator needs from the Kafka producer (an interface so tests can record instead). */
export interface EventSink {
    sendGpsEvents(events: GpsEvent[]): Promise<void>;
    sendDeliveryEvent(event: DeliveryEvent): Promise<void>;
    sendRoutePlan(plan: RoutePlanMessage): Promise<void>;
}

export interface VanSimulatorConfig {
    /** GPS ping interval in wall-clock milliseconds. */
    pingIntervalMs: number;
    /** Simulated seconds per wall-clock second (vans move, dwell and meet deadlines this much faster). */
    timeScale: number;
    /** Free-flow speed in simulated km/h; each tick varies it by ±20%. */
    baseSpeedKmh: number;
    /** Time spent at a stop, simulated seconds. */
    deliveryDurationMinSec: number;
    deliveryDurationMaxSec: number;
    chaosEnabled: boolean;
    /** Router used to compute detours for inserted stops. */
    router?: Router;
    /** Called once the van has emitted its final RETURNED ping. */
    onRouteCompleted?: (vanId: string) => void;
    now?: () => number;
    random?: () => number;
}

const DEFAULT_CONFIG: VanSimulatorConfig = {
    pingIntervalMs: 500,
    timeScale: 4,
    baseSpeedKmh: 25,
    deliveryDurationMinSec: 20,
    deliveryDurationMaxSec: 60,
    chaosEnabled: true,
};

/** Minimum distance ahead of the van at which an inserted detour may start, metres. */
const DETOUR_LEAD_METERS = 150;

export interface StopInsertion {
    /** Insert before this stop index (stops.length = after the last stop). */
    beforeIndex: number;
    location: Waypoint;
    slaDeadline: Date;
    customerId: string;
    parcels?: number;
}

/**
 * Simulates one electric delivery van driving its route.
 *
 *   EN_ROUTE -> DELIVERING -> EN_ROUTE ... -> RETURNING -> RETURNED
 *
 * The van's position is a distance along the route polyline. Each tick it
 * advances by speed x elapsed time x timeScale, slower inside delay zones,
 * and stops exactly at each delivery vertex. GPS pings carry the interpolated
 * position and the simulated speed.
 */
export class VanSimulator {
    private readonly config: VanSimulatorConfig;
    private readonly route: VanRoute;
    private readonly sink: EventSink;
    private readonly now: () => number;
    private readonly random: () => number;

    private status: VanStatus = 'IDLE';
    /** Distance travelled along the polyline, metres. */
    private position = 0;
    /** Segment hint: the van is between waypoints[segment] and waypoints[segment + 1]. */
    private segment = 0;
    private cum: number[];
    private stopIndex = 0;
    private planVersion = 0;
    private sequenceNumber: number;
    private battery = 95 + Math.floor(Math.random() * 6);
    private lastTickAt = 0;
    private deliveringUntil = 0;
    private dwellSimSec = 0;
    private intervalId: ReturnType<typeof setInterval> | null = null;
    private ticking = false;

    private readonly reorderer = new EventReorderer();
    private readonly dropper = new ConnectionDropper();

    constructor(route: VanRoute, sink: EventSink, config: Partial<VanSimulatorConfig> = {}) {
        this.route = route;
        this.sink = sink;
        this.config = { ...DEFAULT_CONFIG, ...config };
        this.now = this.config.now ?? Date.now;
        this.random = this.config.random ?? Math.random;
        this.cum = cumulativeDistances(route.waypoints);
        // Seed with epoch millis so a respawned van keeps increasing sequence
        // numbers (the backend dedups on van_id + sequence_number).
        this.sequenceNumber = this.now();
    }

    // ═══════════════════ Lifecycle ═══════════════════

    /** Puts the van on the road, publishes its plan and ticks every pingIntervalMs. */
    async start(): Promise<void> {
        if (this.intervalId) return;
        await this.begin();
        console.log(`🚐 ${this.route.van_id} started: ${this.route.stops.length} stops, ` +
            `${(this.cum[this.cum.length - 1] / 1000).toFixed(1)} km`);
        this.intervalId = setInterval(() => void this.tick(), this.config.pingIntervalMs);
    }

    /** Puts the van on the road and publishes its plan, without starting the timer. */
    async begin(): Promise<void> {
        this.status = 'EN_ROUTE';
        this.lastTickAt = this.now();
        await this.sink.sendRoutePlan(this.routePlan());
    }

    /** Stops the timer and flushes any GPS events still held by the chaos reorderer. */
    async stop(): Promise<void> {
        if (this.intervalId) {
            clearInterval(this.intervalId);
            this.intervalId = null;
        }
        const remaining = this.reorderer.flush();
        if (remaining.length > 0) {
            await this.sink.sendGpsEvents(remaining);
        }
        if (this.status !== 'RETURNED') this.status = 'IDLE';
    }

    // ═══════════════════ Simulation tick ═══════════════════

    /**
     * One simulation step. All state changes happen synchronously first;
     * the resulting events are sent afterwards, so a stop insertion that runs
     * while a send is in flight always sees a consistent van.
     */
    async tick(): Promise<void> {
        if (this.ticking || this.status === 'IDLE' || this.status === 'RETURNED') return;
        this.ticking = true;
        try {
            const now = this.now();
            const dtSec = Math.min(Math.max(0, now - this.lastTickAt), 4 * this.config.pingIntervalMs) / 1000;
            this.lastTickAt = now;

            const deliveries: DeliveryEvent[] = [];
            let speedKmh = 0;
            let finished = false;

            if (this.status === 'DELIVERING') {
                if (now >= this.deliveringUntil) {
                    deliveries.push(...this.completeDelivery(now));
                    this.stopIndex++;
                    this.status = this.stopIndex < this.route.stops.length ? 'EN_ROUTE' : 'RETURNING';
                }
            } else {
                const limitVertex = this.status === 'EN_ROUTE'
                    ? this.route.stops[this.stopIndex].waypoint_index
                    : this.route.waypoints.length - 1;
                const limit = this.cum[limitVertex];

                if (this.position < limit) {
                    const factor = speedFactorAt(this.location());
                    speedKmh = this.config.baseSpeedKmh * (0.8 + this.random() * 0.4) * factor;
                    const advance = (speedKmh / 3.6) * dtSec * this.config.timeScale;
                    this.battery = Math.max(5, this.battery - advance / 1500);
                    this.moveTo(Math.min(limit, this.position + advance));
                }

                if (this.position >= limit) {
                    this.moveTo(limit, limitVertex);
                    speedKmh = 0;
                    if (this.status === 'EN_ROUTE') {
                        deliveries.push(this.arriveAtStop(now));
                    } else {
                        this.status = 'RETURNED';
                        finished = true;
                    }
                }
            }

            const ping = this.gpsEvent(speedKmh, now);
            for (const d of deliveries) {
                await this.sink.sendDeliveryEvent(d);
            }
            await this.emitGps(ping);

            if (finished) {
                console.log(`✅ ${this.route.van_id} completed route ${this.route.route_id}`);
                await this.stop();
                this.config.onRouteCompleted?.(this.route.van_id);
            }
        } finally {
            this.ticking = false;
        }
    }

    private arriveAtStop(now: number): DeliveryEvent {
        const stop = this.route.stops[this.stopIndex];
        this.status = 'DELIVERING';
        this.dwellSimSec = this.config.deliveryDurationMinSec +
            this.random() * (this.config.deliveryDurationMaxSec - this.config.deliveryDurationMinSec);
        this.deliveringUntil = now + (this.dwellSimSec / this.config.timeScale) * 1000;
        return this.deliveryEvent(stop, 'ARRIVAL', now, 0, 0, null);
    }

    private completeDelivery(now: number): DeliveryEvent[] {
        const stop = this.route.stops[this.stopIndex];
        const success = this.random() < 0.95;
        const duration = Math.round(this.dwellSimSec);
        const outcome = this.deliveryEvent(stop, success ? 'DELIVERY_COMPLETED' : 'DELIVERY_FAILED', now,
            success ? stop.parcels : 0, duration, success ? null : 'Customer not home');
        const departure = this.deliveryEvent(stop, 'DEPARTURE', now, outcome.parcels_delivered, duration, outcome.notes);
        return [outcome, departure];
    }

    // ═══════════════════ Position ═══════════════════

    /** Moves to `distance` metres along the route, keeping the segment hint in sync. */
    private moveTo(distance: number, vertex?: number): void {
        this.position = distance;
        if (vertex !== undefined) {
            this.segment = Math.min(vertex, this.route.waypoints.length - 2);
            return;
        }
        const last = this.route.waypoints.length - 1;
        while (this.segment < last - 1 && this.cum[this.segment + 1] <= distance) this.segment++;
        while (this.segment > 0 && this.cum[this.segment] > distance) this.segment--;
    }

    /** Current interpolated position. */
    location(): Waypoint {
        const wps = this.route.waypoints;
        if (wps.length === 1) return wps[0];
        const i = this.segment;
        const len = this.cum[i + 1] - this.cum[i];
        const t = len > 0 ? Math.min(1, Math.max(0, (this.position - this.cum[i]) / len)) : 0;
        return lerp(wps[i], wps[i + 1], t);
    }

    private heading(): number {
        const wps = this.route.waypoints;
        const i = Math.min(this.segment, wps.length - 2);
        return i >= 0 ? calculateBearing(wps[i], wps[i + 1]) : 0;
    }

    // ═══════════════════ Events ═══════════════════

    private gpsEvent(speedKmh: number, now: number): GpsEvent {
        const loc = this.location();
        return {
            event_id: uuidv4(),
            van_id: this.route.van_id,
            sequence_number: this.sequenceNumber++,
            device_timestamp: new Date(now).toISOString(),
            ingestion_timestamp: null, // set by the Kafka producer
            location: { latitude: loc.latitude, longitude: loc.longitude },
            speed_kmh: Math.round(speedKmh * 10) / 10,
            heading_degrees: Math.round(this.heading() * 10) / 10,
            battery_pct: Math.round(this.battery),
            route_id: this.route.route_id,
            current_stop_index: this.stopIndex,
            total_stops: this.route.stops.length,
            status: this.status,
        };
    }

    private deliveryEvent(stop: RouteStop, type: DeliveryEvent['event_type'], now: number,
        parcels: number, durationSec: number, notes: string | null): DeliveryEvent {
        return {
            event_id: uuidv4(),
            van_id: this.route.van_id,
            route_id: this.route.route_id,
            stop_index: stop.stop_index,
            customer_id: stop.customer_id,
            event_type: type,
            timestamp: new Date(now).toISOString(),
            location: stop.location,
            parcels_delivered: parcels,
            delivery_duration_seconds: durationSec,
            sla_deadline: stop.sla_deadline,
            total_stops: this.route.stops.length,
            notes,
        };
    }

    /** Sends a GPS ping through the chaos modules (drop, duplicate, reorder). */
    private async emitGps(event: GpsEvent): Promise<void> {
        if (!this.config.chaosEnabled) {
            await this.sink.sendGpsEvents([event]);
            return;
        }
        // The final RETURNED ping is never dropped or delayed: it tells the
        // backend the route is over.
        if (event.status === 'RETURNED') {
            const flushed = this.reorderer.flush();
            await this.sink.sendGpsEvents([...flushed, event]);
            return;
        }
        if (this.dropper.shouldDrop()) return;
        for (const e of maybeDuplicate(event)) {
            const flushed = this.reorderer.push(e);
            if (flushed.length > 0) {
                await this.sink.sendGpsEvents(flushed);
            }
        }
    }

    routePlan(): RoutePlanMessage {
        return {
            route_id: this.route.route_id,
            van_id: this.route.van_id,
            version: this.planVersion,
            created_at: this.route.created_at,
            time_scale: this.config.timeScale,
            base_speed_kmh: this.config.baseSpeedKmh,
            stops: this.route.stops.map((s) => ({ ...s, location: { ...s.location } })),
            waypoints: this.route.waypoints.map((w) => [w.latitude, w.longitude]),
        };
    }

    // ═══════════════════ Ad-hoc stops ═══════════════════

    /** The first stop the van has not started serving yet. */
    nextUnvisitedStop(): number {
        return this.status === 'DELIVERING' ? this.stopIndex + 1 : this.stopIndex;
    }

    /**
     * Inserts a stop before `beforeIndex` (clamped so it is never behind the
     * van) and reroutes the section between the previous and the next stop:
     * the vertices of that section are replaced by the detour, so the van
     * never jumps back along its route.
     *
     * If the van is already too close to where the detour would start, the
     * stop goes one position later. Resolves to the index the stop was inserted
     * at, or null if the van can no longer take it.
     */
    async insertStop(req: StopInsertion, attempt = 0): Promise<number | null> {
        if (!this.config.router) throw new Error('insertStop needs a router');
        if (this.status === 'IDLE' || this.status === 'RETURNED') return null;

        const stops = this.route.stops;
        let k = Math.min(Math.max(req.beforeIndex, this.nextUnvisitedStop()), stops.length);
        let fromVertex = this.detourStart(k);
        if (fromVertex === null) {
            if (k >= stops.length) return null; // already heading home and too close to the hub
            k += 1;
            fromVertex = this.detourStart(k);
            if (fromVertex === null) return null;
        }
        const toVertex = k < stops.length ? stops[k].waypoint_index : this.route.waypoints.length - 1;
        const version = this.planVersion;

        const path = await this.config.router.route([
            this.route.waypoints[fromVertex], req.location, this.route.waypoints[toVertex],
        ]);

        // The van kept driving while we waited for the router.
        if (this.planVersion !== version || this.currentStatus === 'RETURNED' || this.position > this.cum[fromVertex]
            || this.nextUnvisitedStop() > k) {
            return attempt < 2 ? this.insertStop({ ...req, beforeIndex: k }, attempt + 1) : null;
        }

        // Replace the vertices strictly between fromVertex and toVertex with the detour's interior.
        const interior = path.waypoints.slice(1, -1);
        let stopOffset = path.legEndIndices[0];
        if (interior.length === 0) {
            interior.push(req.location);
            stopOffset = 1;
        }
        stopOffset = Math.min(Math.max(stopOffset, 1), interior.length);
        const removed = toVertex - fromVertex - 1;
        const delta = interior.length - removed;
        this.route.waypoints.splice(fromVertex + 1, removed, ...interior);
        for (let i = k; i < stops.length; i++) {
            stops[i].waypoint_index += delta;
        }

        this.cum = cumulativeDistances(this.route.waypoints);
        const vertex = fromVertex + stopOffset;
        const etaSec = travelSeconds(this.cum, fromVertex, vertex, this.config.baseSpeedKmh);
        stops.splice(k, 0, {
            stop_index: k,
            customer_id: req.customerId,
            location: req.location,
            sla_deadline: req.slaDeadline.toISOString(),
            planned_arrival: new Date(this.now() + (Math.max(0, etaSec) / this.config.timeScale) * 1000).toISOString(),
            parcels: req.parcels ?? 1,
            waypoint_index: vertex,
        });
        stops.forEach((s, i) => { s.stop_index = i; });
        this.moveTo(this.position);
        this.planVersion++;

        console.log(`⚡ ${this.vanId}: inserted ${req.customerId} as stop ${k + 1}/${stops.length}`);
        await this.sink.sendRoutePlan(this.routePlan());
        return k;
    }

    /**
     * Vertex from which a detour to a stop inserted before `k` starts: the
     * previous stop's vertex, or, when `k` is the stop the van is driving to,
     * the first vertex at least DETOUR_LEAD_METERS ahead of the van. Null when
     * the van is already too close to where the detour would have to start.
     */
    private detourStart(k: number): number | null {
        const stops = this.route.stops;
        const next = this.nextUnvisitedStop();
        const endVertex = k < stops.length ? stops[k].waypoint_index : this.route.waypoints.length - 1;
        if (k > next || (k === next && this.status === 'DELIVERING')) {
            return stops[k - 1].waypoint_index;
        }
        // Detour starts ahead of the van, before the next target.
        const minDistance = this.position + DETOUR_LEAD_METERS;
        for (let v = this.segment + 1; v < endVertex; v++) {
            if (this.cum[v] >= minDistance) return v;
        }
        return null;
    }

    // ═══════════════════ Accessors ═══════════════════

    get vanId(): string {
        return this.route.van_id;
    }

    get currentStatus(): VanStatus {
        return this.status;
    }

    get currentStopIndex(): number {
        return this.stopIndex;
    }

    get stops(): readonly RouteStop[] {
        return this.route.stops;
    }

    get routeId(): string {
        return this.route.route_id;
    }

    getCurrentLocation(): Waypoint {
        return this.location();
    }
}
