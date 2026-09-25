import { Kafka, Consumer } from 'kafkajs';
import { VanSimulator } from './van-simulator.js';
import { haversineDistance } from './geo.js';
import { TOPICS } from './kafka-producer.js';

/**
 * Receives ad-hoc orders from the dashboard (via the backend and Kafka) and
 * inserts each one into the route of the closest van that still has stops to
 * make, just before that van's next stop.
 */
export class DispatchConsumer {
    private kafka: Kafka;
    private consumer: Consumer;

    constructor(brokers: string[], private readonly simulators: VanSimulator[], private readonly timeScale: number) {
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
        console.log('📡 Listening for ad-hoc dispatches');

        await this.consumer.run({
            eachMessage: async ({ message }) => {
                try {
                    if (!message.value) return;
                    const order = JSON.parse(message.value.toString());
                    await this.assign(order.eventId ?? String(Date.now()), order.latitude, order.longitude);
                } catch (e) {
                    console.error('Failed to process dispatch', e);
                }
            },
        });
    }

    private async assign(orderId: string, latitude: number, longitude: number): Promise<void> {
        const target = { latitude, longitude };
        const candidates = this.simulators
            .filter((s) => s.currentStatus === 'EN_ROUTE' || s.currentStatus === 'DELIVERING')
            .sort((a, b) => haversineDistance(a.getCurrentLocation(), target) - haversineDistance(b.getCurrentLocation(), target));

        for (const van of candidates) {
            const inserted = await van.insertStop({
                beforeIndex: van.nextUnvisitedStop(),
                location: target,
                // A 15-minute slot, in simulated time
                slaDeadline: new Date(Date.now() + (15 * 60 * 1000) / this.timeScale),
                customerId: `dispatch-${orderId.slice(0, 8)}`,
            });
            if (inserted !== null) return;
        }
        console.log(`⚠️ No van could take dispatch ${orderId}`);
    }

    async disconnect(): Promise<void> {
        await this.consumer.disconnect();
    }
}
