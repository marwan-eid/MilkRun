import { Kafka, Consumer } from 'kafkajs';
import { VanSimulator } from './van-simulator.js';
import { TOPICS } from './kafka-producer.js';

/**
 * An ad-hoc order assigned by the backend's dispatcher (dispatch-events topic,
 * keyed by van id). The backend chose the van and the position in its route.
 */
export interface DispatchAssignment {
    order_id: string;
    van_id: string;
    route_id: string;
    latitude: number;
    longitude: number;
    /** Insert before this stop index (== stop count: after the last stop). */
    insert_before: number;
    sla_deadline: string;
}

export type ApplyResult =
    | { status: 'inserted'; vanId: string; index: number }
    | { status: 'rejected'; reason: string };

/**
 * Inserts the order into the assigned van's route. The van may have moved on
 * since the backend decided (finished the route, passed the position); the
 * van then inserts as close to the requested position as it still can, or
 * rejects the order.
 */
export async function applyAssignment(simulators: readonly VanSimulator[], a: DispatchAssignment): Promise<ApplyResult> {
    if (!a || typeof a.van_id !== 'string' || typeof a.latitude !== 'number' || typeof a.longitude !== 'number'
        || typeof a.insert_before !== 'number' || !a.sla_deadline) {
        return { status: 'rejected', reason: 'malformed assignment' };
    }
    const van = simulators.find((s) => s.vanId === a.van_id);
    if (!van) {
        return { status: 'rejected', reason: `${a.van_id} is not running` };
    }
    if (van.routeId !== a.route_id) {
        return { status: 'rejected', reason: `${a.van_id} has moved on to another route` };
    }
    const index = await van.insertStop({
        beforeIndex: a.insert_before,
        location: { latitude: a.latitude, longitude: a.longitude },
        slaDeadline: new Date(a.sla_deadline),
        customerId: `order-${String(a.order_id).slice(0, 8)}`,
    });
    return index === null
        ? { status: 'rejected', reason: `${a.van_id} can no longer take stops` }
        : { status: 'inserted', vanId: a.van_id, index };
}

/** Consumes dispatch assignments and applies them to the simulated vans. */
export class DispatchConsumer {
    private kafka: Kafka;
    private consumer: Consumer;

    constructor(brokers: string[], private readonly simulators: VanSimulator[]) {
        this.kafka = new Kafka({ clientId: 'simulator-dispatch-consumer', brokers });
        this.consumer = this.kafka.consumer({ groupId: 'simulator-dispatch-group' });

        // If the consumer dies, exit so Docker's restart policy brings the
        // simulator back instead of silently ignoring dispatches.
        this.consumer.on(this.consumer.events.CRASH, (e) => {
            console.error('💥 Dispatch consumer crashed, exiting so the container restarts', e);
            process.exit(1);
        });
    }

    async connect(): Promise<void> {
        await this.consumer.connect();
        // Only new orders: replaying old dispatches after a restart would send
        // vans to stale locations.
        await this.consumer.subscribe({ topic: TOPICS.dispatch, fromBeginning: false });
        console.log('📡 Listening for dispatch assignments');

        await this.consumer.run({
            eachMessage: async ({ message }) => {
                if (!message.value) return;
                try {
                    const result = await applyAssignment(this.simulators, JSON.parse(message.value.toString()));
                    if (result.status === 'rejected') {
                        console.log(`⚠️ Dispatch rejected: ${result.reason}`);
                    }
                } catch (e) {
                    console.error('Failed to apply dispatch assignment', e);
                }
            },
        });
    }

    async disconnect(): Promise<void> {
        await this.consumer.disconnect();
    }
}
