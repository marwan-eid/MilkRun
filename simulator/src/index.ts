import { KafkaEventProducer } from './kafka-producer.js';
import { generateRoute } from './route-generator.js';
import { VanSimulator } from './van-simulator.js';
import { DispatchConsumer } from './dispatch-consumer.js';

/**
 * The Milk-Run Fleet Simulator
 *
 * Launches N virtual delivery vans that broadcast GPS pings and delivery
 * events into Kafka, simulating Picnic's milkman-style delivery fleet.
 *
 * Usage:
 *   npm start                          # 50 vans, chaos enabled
 *   VAN_COUNT=10 npm start             # 10 vans
 *   KAFKA_BROKERS=kafka:9092 npm start # custom broker
 *   CHAOS_ENABLED=false npm start      # disable chaos modules
 */

// ═══════════════════════════════════════════════
// Configuration from environment
// ═══════════════════════════════════════════════
const VAN_COUNT = parseInt(process.env.VAN_COUNT || '50', 10);
const STOPS_PER_VAN = parseInt(process.env.STOPS_PER_VAN || '18', 10);
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const CHAOS_ENABLED = process.env.CHAOS_ENABLED !== 'false';
const PING_INTERVAL_MS = parseInt(process.env.PING_INTERVAL_MS || '500', 10);
const STATS_INTERVAL_MS = parseInt(process.env.STATS_INTERVAL_MS || '5000', 10);

console.log(`
╔═══════════════════════════════════════════════════════╗
║          🥛 The Milk-Run Fleet Simulator 🚐           ║
╠═══════════════════════════════════════════════════════╣
║  Vans:          ${String(VAN_COUNT).padStart(4)}                                ║
║  Stops/Van:     ${String(STOPS_PER_VAN).padStart(4)} (±4)                          ║
║  Ping Rate:     ${String(PING_INTERVAL_MS).padStart(4)}ms                            ║
║  Chaos:         ${CHAOS_ENABLED ? ' ON ✨' : 'OFF   '}                            ║
║  Kafka:         ${KAFKA_BROKERS[0].padEnd(32)}    ║
╚═══════════════════════════════════════════════════════╝
`);

async function main(): Promise<void> {
    // 1. Connect to Kafka FIRST so we can enable rolling dispatch
    const producer = new KafkaEventProducer(KAFKA_BROKERS);
    await producer.connect();

    // 2. State management for active vans
    const simulators: VanSimulator[] = [];

    // Initialize instantaneous Map Map Interface dispatcher
    const dispatchConsumer = new DispatchConsumer(KAFKA_BROKERS, simulators);
    await dispatchConsumer.connect();

    // 3. Recursive Engine: dynamically orchestrates, builds and respawns individual vans forever natively 
    const deployVan = async (vanIndex: number) => {
        const stops = STOPS_PER_VAN - 4 + Math.floor(Math.random() * 9);
        const route = await generateRoute(vanIndex, stops);

        const sim = new VanSimulator(route, producer, {
            pingIntervalMs: PING_INTERVAL_MS,
            chaosEnabled: CHAOS_ENABLED,
            onRouteCompleted: async (vanId) => {
                console.log(`♻️  Cycling Van Pipeline: Respawning ${vanId} out to a new neighborhood...`);
                // Physically surgically slice the old van exactly out of Javascript V8 Node Memory!
                const idx = simulators.findIndex(s => s.vanId === vanId);
                if (idx > -1) {
                    simulators.splice(idx, 1);
                }
                // 1-second algorithmic breather before launching the next physical shift!
                setTimeout(() => deployVan(vanIndex), 1000);
            }
        });

        simulators.push(sim);
        sim.start();  // Spin up Kafka threads
    };

    console.log(`\n🗺️  Generating ${VAN_COUNT} Perpetual routes across Amsterdam (Infinite Loop Active)...`);
    for (let i = 0; i < VAN_COUNT; i++) {
        await deployVan(i);
        if (i % 5 === 0) {
            console.log(`   ... locked geometries and mounted ${i + 1}/${VAN_COUNT} vans dynamically`);
        }
        // Force the same OSRM Anti-Spam stagger
        if (i < VAN_COUNT - 1) {
            await new Promise(r => setTimeout(r, 1500));
        }
    }


    // 4. Stats reporter
    const statsInterval = setInterval(() => {
        const stats = producer.stats;
        const activeVans = simulators.filter(
            (s) => s.currentStatus !== 'IDLE',
        ).length;

        console.log(
            `📊 [${new Date().toISOString().slice(11, 19)}] ` +
            `Active: ${activeVans}/${VAN_COUNT} | ` +
            `GPS sent: ${stats.gpsSent} | ` +
            `Deliveries: ${stats.deliverySent} | ` +
            `Errors: ${stats.errors}`,
        );
    }, STATS_INTERVAL_MS);

    // 5. Graceful shutdown
    const shutdown = async (signal: string) => {
        console.log(`\n⏹️  Received ${signal}, shutting down gracefully...`);
        clearInterval(statsInterval);

        for (const sim of simulators) {
            sim.stop();
        }

        await dispatchConsumer.disconnect();
        await producer.disconnect();
        console.log('👋 Simulator stopped. Goodbye!');
        process.exit(0);
    };

    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('SIGTERM', () => shutdown('SIGTERM'));
}

main().catch((err) => {
    console.error('💥 Fatal error:', err);
    process.exit(1);
});
