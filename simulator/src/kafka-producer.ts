import { Kafka, Producer, Partitioners, CompressionTypes } from 'kafkajs';
import { type GpsEvent, type DeliveryEvent } from './models/index.js';

/**
 * Kafka producer wrapper for the fleet simulator.
 *
 * Uses KafkaJS with:
 * - Partitioning by van_id (co-locates all events for a van in one partition)
 * - GZIP compression for throughput
 * - Ingestion timestamp injection
 */
export class KafkaEventProducer {
    private kafka: Kafka;
    private producer: Producer;
    private connected = false;

    // Metrics
    private _gpsSent = 0;
    private _deliverySent = 0;
    private _errors = 0;

    constructor(
        private readonly brokers: string[] = ['localhost:9092'],
        private readonly gpsTopic: string = 'gps-events',
        private readonly deliveryTopic: string = 'delivery-events',
    ) {
        this.kafka = new Kafka({
            clientId: 'milkrun-simulator',
            brokers: this.brokers,
            retry: {
                initialRetryTime: 300,
                retries: 5,
            },
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

    /**
     * Send one or more GPS events (supports batching from chaos duplicator).
     */
    async sendGpsEvents(events: GpsEvent[]): Promise<void> {
        if (events.length === 0) return;

        try {
            await this.producer.send({
                topic: this.gpsTopic,
                compression: CompressionTypes.GZIP,
                messages: events.map((event) => ({
                    key: event.van_id,
                    value: JSON.stringify({
                        ...event,
                        ingestion_timestamp: new Date().toISOString(),
                    }),
                    headers: {
                        'event-type': 'GPS_PING',
                        'van-id': event.van_id,
                    },
                })),
            });

            this._gpsSent += events.length;
        } catch (err) {
            this._errors++;
            console.error(`❌ Failed to send GPS events: ${(err as Error).message}`);
        }
    }

    /**
     * Send a delivery event (arrival, completion, departure, etc.).
     */
    async sendDeliveryEvent(event: DeliveryEvent): Promise<void> {
        try {
            await this.producer.send({
                topic: this.deliveryTopic,
                compression: CompressionTypes.GZIP,
                messages: [
                    {
                        key: event.van_id,
                        value: JSON.stringify(event),
                        headers: {
                            'event-type': event.event_type,
                            'van-id': event.van_id,
                            'route-id': event.route_id,
                        },
                    },
                ],
            });

            this._deliverySent++;
        } catch (err) {
            this._errors++;
            console.error(`❌ Failed to send delivery event: ${(err as Error).message}`);
        }
    }

    get stats() {
        return {
            gpsSent: this._gpsSent,
            deliverySent: this._deliverySent,
            errors: this._errors,
        };
    }
}
