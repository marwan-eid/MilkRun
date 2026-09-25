import { KafkaEventProducer } from './kafka-producer.js';
import { generateRoute, OsrmRouter, StraightLineRouter, type Router } from './route-generator.js';
import { VanSimulator } from './van-simulator.js';
import { DispatchConsumer } from './dispatch-consumer.js';

/**
 * The Milk-Run Fleet Simulator
 *
 * Runs N virtual delivery vans that publish GPS pings, delivery events and
 * route plans to Kafka.
 *
 * Usage:
 *   npm start                          # 50 vans, chaos enabled
 *   VAN_COUNT=10 npm start             # 10 vans
 *   KAFKA_BROKERS=kafka:9092 npm start # custom broker
 *   CHAOS_ENABLED=false npm start      # disable chaos modules
 *   ROUTER=straight npm start          # no OSRM calls (straight-line streets)
 */

const VAN_COUNT = parseInt(process.env.VAN_COUNT || '50', 10);
const STOPS_PER_VAN = parseInt(process.env.STOPS_PER_VAN || '18', 10);
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const CHAOS_ENABLED = process.env.CHAOS_ENABLED !== 'false';
const PING_INTERVAL_MS = parseInt(process.env.PING_INTERVAL_MS || '500', 10);
const STATS_INTERVAL_MS = parseInt(process.env.STATS_INTERVAL_MS || '5000', 10);
const TIME_SCALE = parseFloat(process.env.TIME_SCALE || '4');
const BASE_SPEED_KMH = parseFloat(process.env.BASE_SPEED_KMH || '25');
// Pause between route requests: the public OSRM server allows about one per second.
const ROUTE_STAGGER_MS = parseInt(process.env.ROUTE_STAGGER_MS || '1500', 10);

const router: Router = process.env.ROUTER === 'straight' ? new StraightLineRouter() : new OsrmRouter();

console.log(`
╔═══════════════════════════════════════════════════════╗
║          🥛 The Milk-Run Fleet Simulator 🚐           ║
╠═══════════════════════════════════════════════════════╣
║  Vans:          ${String(VAN_COUNT).padStart(4)}                                  ║
║  Stops/Van:     ${String(STOPS_PER_VAN).padStart(4)} (±4)                             ║
║  Ping Rate:     ${String(PING_INTERVAL_MS).padStart(4)}ms                                ║
║  Time scale:    ${String(TIME_SCALE).padStart(4)}x                                 ║
║  Chaos:         ${CHAOS_ENABLED ? ' ON ✨' : 'OFF   '}                                ║
║  Kafka:         ${KAFKA_BROKERS[0].padEnd(32)}      ║
╚═══════════════════════════════════════════════════════╝
`);

async function main(): Promise<void> {
    const producer = new KafkaEventProducer(KAFKA_BROKERS);
    await producer.connect();

    const simulators: VanSimulator[] = [];
    const dispatchConsumer = new DispatchConsumer(KAFKA_BROKERS, simulators, TIME_SCALE);
    await dispatchConsumer.connect();

    // Deploy a van; when it finishes its route, deploy a fresh route for the same van.
    const deployVan = async (vanIndex: number) => {
        const stops = STOPS_PER_VAN - 4 + Math.floor(Math.random() * 9);
        const route = await generateRoute(vanIndex, stops, {
            router, timeScale: TIME_SCALE, baseSpeedKmh: BASE_SPEED_KMH,
        });

        const sim = new VanSimulator(route, producer, {
            pingIntervalMs: PING_INTERVAL_MS,
            chaosEnabled: CHAOS_ENABLED,
            timeScale: TIME_SCALE,
            baseSpeedKmh: BASE_SPEED_KMH,
            router,
            onRouteCompleted: (vanId) => {
                const idx = simulators.findIndex((s) => s.vanId === vanId);
                if (idx > -1) simulators.splice(idx, 1);
                console.log(`♻️  ${vanId} back at the hub, starting a new route`);
                setTimeout(() => void deployVan(vanIndex).catch((e) => console.error(`Redeploying ${vanId} failed`, e)), 1000);
            },
        });

        simulators.push(sim);
        await sim.start();
    };

    console.log(`\n🗺️  Generating ${VAN_COUNT} routes across Amsterdam (vans restart on completion)...`);
    for (let i = 0; i < VAN_COUNT; i++) {
        await deployVan(i);
        if (i % 5 === 0) {
            console.log(`   ... ${i + 1}/${VAN_COUNT} vans on the road`);
        }
        if (i < VAN_COUNT - 1) {
            await new Promise((r) => setTimeout(r, ROUTE_STAGGER_MS));
        }
    }

    const statsInterval = setInterval(() => {
        const stats = producer.stats;
        const active = simulators.filter((s) => s.currentStatus !== 'IDLE').length;
        console.log(
            `📊 [${new Date().toISOString().slice(11, 19)}] ` +
            `Active: ${active}/${VAN_COUNT} | GPS sent: ${stats.gpsSent} | ` +
            `Deliveries: ${stats.deliverySent} | Plans: ${stats.plansSent} | Errors: ${stats.errors}`,
        );
    }, STATS_INTERVAL_MS);

    const shutdown = async (signal: string) => {
        console.log(`\n⏹️  Received ${signal}, shutting down...`);
        clearInterval(statsInterval);
        await Promise.all(simulators.map((s) => s.stop()));
        await dispatchConsumer.disconnect();
        await producer.disconnect();
        console.log('👋 Simulator stopped.');
        process.exit(0);
    };

    process.on('SIGINT', () => void shutdown('SIGINT'));
    process.on('SIGTERM', () => void shutdown('SIGTERM'));
}

main().catch((err) => {
    console.error('💥 Fatal error:', err);
    process.exit(1);
});
