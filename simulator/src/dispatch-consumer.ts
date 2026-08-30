import { Kafka, Consumer } from 'kafkajs';
import { VanSimulator } from './van-simulator.js';
import { haversineDistance } from './route-generator.js';

export class DispatchConsumer {
    private kafka: Kafka;
    private consumer: Consumer;
    private simulators: VanSimulator[];

    constructor(brokers: string[], simulators: VanSimulator[]) {
        this.kafka = new Kafka({
            clientId: 'simulator-dispatch-consumer',
            brokers
        });
        this.consumer = this.kafka.consumer({ groupId: 'simulator-dispatch-group' });
        this.simulators = simulators;
    }

    public async connect(): Promise<void> {
        // Resolve race condition where NodeJS beats Spring Boot natively to the Kafka Cluster
        const admin = this.kafka.admin();
        await admin.connect();
        const topics = await admin.listTopics();
        if (!topics.includes('dispatch-events')) {
            // Attempt an explicit raw partition allocation
            try {
                await admin.createTopics({ topics: [{ topic: 'dispatch-events' }] });
                console.log('🏗️ [DISPATCH DAEMON] Natively partitioned dispatch-events topic.');
            } catch (ignored) { }
        }
        await admin.disconnect();

        await this.consumer.connect();
        await this.consumer.subscribe({ topic: 'dispatch-events', fromBeginning: false });

        console.log('📡 [DISPATCH DAEMON] Subscribed and listening for ad-hoc UI map injections...');

        await this.consumer.run({
            eachMessage: async ({ message }) => {
                try {
                    if (!message.value) return;
                    const payload = JSON.parse(message.value.toString());
                    const { latitude, longitude } = payload;

                    let closestVan: VanSimulator | null = null;
                    let minDistance = Infinity;

                    // Trace all active operating routes globally to mathematically isolate the most performant redirect
                    for (const sim of this.simulators) {
                        if (sim.currentStatus === 'IDLE' || sim.currentStatus === 'RETURNED' || sim.currentStatus === 'RETURNING') {
                            continue;
                        }
                        const loc = sim.getCurrentLocation();
                        if (!loc) continue;

                        const dist = haversineDistance(loc, { latitude, longitude });
                        if (dist < minDistance) {
                            closestVan = sim;
                            minDistance = dist;
                        }
                    }

                    if (closestVan) {
                        await closestVan.injectAdHocStop(latitude, longitude);
                    } else {
                        console.log('⚠️ [DYNAMIC DISPATCH ERROR] All drivers have retired to the regional hubs. Payload orphaned.');
                    }
                } catch (e) {
                    console.error('Failed processing dynamic dispatch payload', e);
                }
            }
        });
    }

    public async disconnect(): Promise<void> {
        await this.consumer.disconnect();
    }
}
