import { v4 as uuidv4 } from 'uuid';
import { type GpsEvent, type DeliveryEvent, type VanRoute, type VanStatus } from './models/index.js';
import { calculateBearing } from './route-generator.js';
import { maybeDuplicate, EventReorderer, ConnectionDropper } from './chaos/index.js';
import { KafkaEventProducer } from './kafka-producer.js';

/**
 * Configuration for a single van simulator.
 */
export interface VanSimulatorConfig {
    /** GPS ping interval in milliseconds. Default: 500 (2 Hz) */
    pingIntervalMs: number;
    /** Base speed in km/h when EN_ROUTE. Default: 25 */
    baseSpeedKmh: number;
    /** How many seconds a delivery stop takes (min). Default: 20 */
    deliveryDurationMinSec: number;
    /** How many seconds a delivery stop takes (max). Default: 60 */
    deliveryDurationMaxSec: number;
    /** Whether to enable chaos modules. Default: true */
    chaosEnabled: boolean;
}

const DEFAULT_CONFIG: VanSimulatorConfig = {
    pingIntervalMs: 500,
    baseSpeedKmh: 25,
    deliveryDurationMinSec: 20,
    deliveryDurationMaxSec: 60,
    chaosEnabled: true,
};

/**
 * Simulates a single electric delivery van traversing its route.
 *
 * State machine:
 *   IDLE → EN_ROUTE → DELIVERING → EN_ROUTE → ... → RETURNING → IDLE
 *
 * Each tick (500ms):
 *   1. Advance position along waypoints based on speed
 *   2. Emit GPS event (possibly duplicated/reordered/dropped by chaos)
 *   3. Check if near next delivery stop → transition to DELIVERING
 *   4. After delivery duration → emit delivery events → resume EN_ROUTE
 */
export class VanSimulator {
    private config: VanSimulatorConfig;
    private route: VanRoute;
    private producer: KafkaEventProducer;

    // State
    private status: VanStatus = 'IDLE';
    private waypointIndex = 0;
    private sequenceNumber = 0;
    private currentStopIndex = 0;
    private batteryPct = 95 + Math.floor(Math.random() * 6); // 95-100%
    private intervalId: ReturnType<typeof setInterval> | null = null;
    private deliveringUntil: number = 0; // timestamp when delivery completes

    // Chaos modules
    private reorderer: EventReorderer;
    private dropper: ConnectionDropper;

    // Track which waypoint corresponds to which stop for triggering deliveries
    private stopWaypointIndices: number[] = [];

    constructor(
        route: VanRoute,
        producer: KafkaEventProducer,
        config: Partial<VanSimulatorConfig> = {},
    ) {
        this.route = route;
        this.producer = producer;
        this.config = { ...DEFAULT_CONFIG, ...config };
        this.reorderer = new EventReorderer();
        this.dropper = new ConnectionDropper();

        // Pre-calculate which waypoint is closest to each delivery stop
        this.preCalculateStopWaypoints();
    }

    /**
     * Map each delivery stop to the nearest waypoint index.
     */
    private preCalculateStopWaypoints(): void {
        for (const stop of this.route.stops) {
            let minDist = Infinity;
            let bestIdx = 0;

            for (let i = 0; i < this.route.waypoints.length; i++) {
                const wp = this.route.waypoints[i];
                const dist = Math.abs(wp.latitude - stop.location.latitude) +
                    Math.abs(wp.longitude - stop.location.longitude);
                if (dist < minDist) {
                    minDist = dist;
                    bestIdx = i;
                }
            }

            this.stopWaypointIndices.push(bestIdx);
        }
    }

    /**
     * Start the simulation loop.
     */
    start(): void {
        if (this.intervalId) return;
        this.status = 'EN_ROUTE';

        console.log(
            `🚐 ${this.route.van_id} started — ${this.route.stops.length} stops, ` +
            `${this.route.waypoints.length} waypoints`
        );

        this.intervalId = setInterval(() => this.tick(), this.config.pingIntervalMs);
    }

    /**
     * Stop the van simulation cleanly by flushing the final events.
     */
    public async stop(): Promise<void> {
        if (this.intervalId) {
            clearInterval(this.intervalId);
            this.intervalId = null;
        }
        // Flush any remaining buffered events
        const remaining = this.reorderer.flush();
        if (remaining.length > 0) {
            await this.producer.sendGpsEvents(remaining);
        }
        this.status = 'IDLE';
    }

    /**
     * Main simulation tick — runs every `pingIntervalMs`.
     */
    private async tick(): Promise<void> {
        const now = Date.now();

        // Handle DELIVERING state — wait for delivery to complete
        if (this.status === 'DELIVERING') {
            if (now >= this.deliveringUntil) {
                await this.completeDelivery();
                this.status = this.currentStopIndex >= this.route.stops.length ? 'RETURNING' : 'EN_ROUTE';
            }
            // Still emit GPS pings while stationary (speed = 0)
            await this.emitGpsPing(0);
            return;
        }

        // Advance along waypoints
        if (this.waypointIndex < this.route.waypoints.length - 1) {
            // Speed with some variation (±20%)
            const speedVariation = 0.8 + Math.random() * 0.4;
            const effectiveSpeed = this.config.baseSpeedKmh * speedVariation;

            // How many waypoints to skip this tick (based on speed)
            const waypointsPerTick = Math.max(1, Math.round(effectiveSpeed / 30));
            this.waypointIndex = Math.min(
                this.waypointIndex + waypointsPerTick,
                this.route.waypoints.length - 1,
            );

            // Drain battery (very slowly)
            if (Math.random() < 0.01) {
                this.batteryPct = Math.max(5, this.batteryPct - 1);
            }

            // Check if we've reached the next delivery stop
            if (
                this.currentStopIndex < this.route.stops.length &&
                this.waypointIndex >= this.stopWaypointIndices[this.currentStopIndex]
            ) {
                await this.arriveAtStop();
                return;
            }

            await this.emitGpsPing(effectiveSpeed);
        } else {
            // Route complete — emit final RETURNED ping
            this.status = 'RETURNED';
            await this.emitGpsPing(0);
            console.log(`✅ ${this.route.van_id} completed route`);
            await this.stop();
        }
    }

    /**
     * Arrive at a delivery stop — transition to DELIVERING.
     */
    private async arriveAtStop(): Promise<void> {
        const stop = this.route.stops[this.currentStopIndex];
        this.status = 'DELIVERING';

        // Random delivery duration
        const duration =
            (this.config.deliveryDurationMinSec +
                Math.random() * (this.config.deliveryDurationMaxSec - this.config.deliveryDurationMinSec)) *
            1000;
        this.deliveringUntil = Date.now() + duration;

        // Emit ARRIVAL delivery event
        const arrivalEvent: DeliveryEvent = {
            event_id: uuidv4(),
            van_id: this.route.van_id,
            route_id: this.route.route_id,
            stop_index: stop.stop_index,
            customer_id: stop.customer_id,
            event_type: 'ARRIVAL',
            timestamp: new Date().toISOString(),
            location: stop.location,
            parcels_delivered: 0,
            delivery_duration_seconds: 0,
            sla_deadline: stop.sla_deadline,
            total_stops: this.route.stops.length,
            notes: null,
        };

        await this.producer.sendDeliveryEvent(arrivalEvent);
        await this.emitGpsPing(0); // Stationary ping
    }

    /**
     * Complete the delivery at the current stop.
     */
    private async completeDelivery(): Promise<void> {
        const stop = this.route.stops[this.currentStopIndex];

        // 95% success rate
        const success = Math.random() < 0.95;
        const eventType = success ? 'DELIVERY_COMPLETED' : 'DELIVERY_FAILED';

        const deliveryEvent: DeliveryEvent = {
            event_id: uuidv4(),
            van_id: this.route.van_id,
            route_id: this.route.route_id,
            stop_index: stop.stop_index,
            customer_id: stop.customer_id,
            event_type: eventType,
            timestamp: new Date().toISOString(),
            location: stop.location,
            parcels_delivered: success ? stop.parcels : 0,
            delivery_duration_seconds: Math.round((this.deliveringUntil - Date.now()) / 1000 + 30),
            sla_deadline: stop.sla_deadline,
            total_stops: this.route.stops.length,
            notes: success ? null : 'Customer not home',
        };

        await this.producer.sendDeliveryEvent(deliveryEvent);

        // Emit DEPARTURE event
        const departureEvent: DeliveryEvent = {
            ...deliveryEvent,
            event_id: uuidv4(),
            event_type: 'DEPARTURE',
            timestamp: new Date().toISOString(),
        };
        await this.producer.sendDeliveryEvent(departureEvent);

        this.currentStopIndex++;
    }

    /**
     * Emit a GPS ping event through the chaos pipeline.
     */
    private async emitGpsPing(speedKmh: number): Promise<void> {
        const wp = this.route.waypoints[this.waypointIndex];
        const nextWp = this.route.waypoints[Math.min(this.waypointIndex + 1, this.route.waypoints.length - 1)];

        const event: GpsEvent = {
            event_id: uuidv4(),
            van_id: this.route.van_id,
            sequence_number: this.sequenceNumber++,
            device_timestamp: new Date().toISOString(),
            ingestion_timestamp: null, // set by Kafka producer
            location: {
                latitude: wp.latitude,
                longitude: wp.longitude,
            },
            speed_kmh: Math.round(speedKmh * 10) / 10,
            heading_degrees: Math.round(calculateBearing(wp, nextWp) * 10) / 10,
            battery_pct: this.batteryPct,
            route_id: this.route.route_id,
            current_stop_index: this.currentStopIndex,
            total_stops: this.route.stops.length,
            status: this.status,
        };

        // === Chaos pipeline ===
        if (this.config.chaosEnabled) {
            // 1. Connection drop check (bypass if it's the critical final RETURNED ping)
            if (this.status !== 'RETURNED' && this.dropper.shouldDrop()) {
                return; // Event silently dropped (simulating connection loss)
            }

            // 2. Duplication
            const events = maybeDuplicate(event);

            // 3. Reordering — buffer and flush
            for (const e of events) {
                if (e.status === 'RETURNED') {
                    // Force flush the buffer first to maintain chronological finality
                    const flushed = this.reorderer.flush();
                    if (flushed.length > 0) {
                        await this.producer.sendGpsEvents(flushed);
                    }
                    // Send RETURNED instantly bypassing the buffer shuffle
                    await this.producer.sendGpsEvents([e]);
                } else {
                    const flushed = this.reorderer.push(e);
                    if (flushed.length > 0) {
                        await this.producer.sendGpsEvents(flushed);
                    }
                }
            }
        } else {
            await this.producer.sendGpsEvents([event]);
        }
    }

    get vanId(): string {
        return this.route.van_id;
    }

    get currentStatus(): VanStatus {
        return this.status;
    }
}
