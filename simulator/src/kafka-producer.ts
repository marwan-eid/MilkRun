import { Kafka, Producer, Partitioners, CompressionTypes, type ITopicConfig } from 'kafkajs';
import { type GpsEvent, type DeliveryEvent, type RoutePlanMessage } from './models/index.js';
import { type EventSink } from './van-simulator.js';

export const TOPICS = {
    gps: 'gps-events',
    delivery: 'delivery-events',
    routePlans: 'route-plans',
    dispatch: 'dispatch-events',
} as const;

/**
 * Topics this simulator writes to or reads from. The backend declares the
 * same topics; whichever process starts first creates them.
 */
const REQUIRED_TOPICS: ITopicConfig[] = [
    { topic: TOPICS.gps, numPartitions: 10 },
    { topic: TOPICS.delivery, numPartitions: 10 },
    {
        topic: TOPICS.routePlans,
        numPartitions: 10,
        // One record per van is enough: the latest plan replaces older ones.
        configEntries: [
            { name: 'cleanup.policy', value: 'compact' },
            { name: 'segment.ms', value: '600000' },
        ],
    },
    { topic: TOPICS.dispatch, numPartitions: 1 },
];

/**
 * Kafka producer for the simulator.
 *
 * - Keyed by van_id, so each van's events stay ordered within one partition
 * - Idempotent producer, GZIP compression
 * - Sets ingestion_timestamp when a GPS event is sent
 */
export class KafkaEventProducer implements EventSink {
    private kafka: Kafka;
    private producer: Producer;
    private connected = false;

    private _gpsSent = 0;
    private _deliverySent = 0;
    private _plansSent = 0;
    private _errors = 0;

    constructor(private readonly brokers: string[] = ['localhost:9092']) {
        this.kafka = new Kafka({
            clientId: 'milkrun-simulator',
            brokers: this.brokers,
            retry: { initialRetryTime: 300, retries: 5 },
        });
        this.producer = this.kafka.producer({
            createPartitioner: Partitioners.DefaultPartitioner,
            allowAutoTopicCreation: false,
            idempotent: true,
            maxInFlightRequests: 5,
        });
    }

    async connect(): Promise<void> {
        if (this.connected) return;
        await this.ensureTopics();
        await this.producer.connect();
        this.connected = true;
        console.log(`📡 Kafka producer connected to ${this.brokers.join(', ')}`);
    }

    async disconnect(): Promise<void> {
        if (!this.connected) return;
        await this.producer.disconnect();
        this.connected = false;
        console.log('📡 Kafka producer disconnected');
    }

    private async ensureTopics(): Promise<void> {
        const admin = this.kafka.admin();
        await admin.connect();
        try {
            const existing = new Set(await admin.listTopics());
            const missing = REQUIRED_TOPICS.filter((t) => !existing.has(t.topic));
            if (missing.length > 0) {
                await admin.createTopics({ topics: missing, waitForLeaders: true });
                console.log(`🏗️  Created topics: ${missing.map((t) => t.topic).join(', ')}`);
            }
        } catch (e) {
            // Another process may have created them at the same moment.
            console.warn(`Topic creation: ${(e as Error).message}`);
        } finally {
            await admin.disconnect();
        }
    }

    async sendGpsEvents(events: GpsEvent[]): Promise<void> {
        if (events.length === 0) return;
        try {
            await this.producer.send({
                topic: TOPICS.gps,
                compression: CompressionTypes.GZIP,
                messages: events.map((event) => ({
                    key: event.van_id,
                    value: JSON.stringify({ ...event, ingestion_timestamp: new Date().toISOString() }),
                    headers: { 'event-type': 'GPS_PING', 'van-id': event.van_id },
                })),
            });
            this._gpsSent += events.length;
        } catch (err) {
            this._errors++;
            console.error(`❌ Failed to send GPS events: ${(err as Error).message}`);
        }
    }

    async sendDeliveryEvent(event: DeliveryEvent): Promise<void> {
        try {
            await this.producer.send({
                topic: TOPICS.delivery,
                compression: CompressionTypes.GZIP,
                messages: [{
                    key: event.van_id,
                    value: JSON.stringify(event),
                    headers: { 'event-type': event.event_type, 'van-id': event.van_id, 'route-id': event.route_id },
                }],
            });
            this._deliverySent++;
        } catch (err) {
            this._errors++;
            console.error(`❌ Failed to send delivery event: ${(err as Error).message}`);
        }
    }

    async sendRoutePlan(plan: RoutePlanMessage): Promise<void> {
        try {
            await this.producer.send({
                topic: TOPICS.routePlans,
                compression: CompressionTypes.GZIP,
                messages: [{
                    key: plan.van_id,
                    value: JSON.stringify(plan),
                    headers: { 'route-id': plan.route_id, 'version': String(plan.version) },
                }],
            });
            this._plansSent++;
        } catch (err) {
            this._errors++;
            console.error(`❌ Failed to send route plan: ${(err as Error).message}`);
        }
    }

    get stats() {
        return {
            gpsSent: this._gpsSent,
            deliverySent: this._deliverySent,
            plansSent: this._plansSent,
            errors: this._errors,
        };
    }
}
