import { describe, it, expect, beforeEach } from 'vitest';
import { VanSimulator, type EventSink } from '../src/van-simulator.js';
import { straightLinePath, StraightLineRouter, type Router, type RoutedPath } from '../src/route-generator.js';
import { haversineMeters, pointInRing, DELAY_ZONES } from '../src/geo.js';
import {
    type GpsEvent, type DeliveryEvent, type RoutePlanMessage, type VanRoute, type Waypoint,
} from '../src/models/index.js';

class RecordingSink implements EventSink {
    gps: GpsEvent[] = [];
    deliveries: DeliveryEvent[] = [];
    plans: RoutePlanMessage[] = [];
    async sendGpsEvents(events: GpsEvent[]) { this.gps.push(...events); }
    async sendDeliveryEvent(event: DeliveryEvent) { this.deliveries.push(event); }
    async sendRoutePlan(plan: RoutePlanMessage) { this.plans.push(JSON.parse(JSON.stringify(plan))); }
}

/** Route through `points`: first is the hub, last is the return point, the rest are stops. */
function makeRoute(points: Waypoint[]): VanRoute {
    const path = straightLinePath(points);
    const far = new Date(Date.UTC(2030, 0, 1)).toISOString();
    return {
        route_id: 'route-test',
        van_id: 'van-test',
        created_at: new Date(0).toISOString(),
        waypoints: path.waypoints,
        stops: points.slice(1, -1).map((location, i) => ({
            stop_index: i,
            customer_id: `cust-${i}`,
            location,
            sla_deadline: far,
            planned_arrival: far,
            parcels: 2,
            waypoint_index: path.legEndIndices[i],
        })),
    };
}

// South of the city, away from every delay zone
const PLAIN: Waypoint[] = [
    { latitude: 52.3000, longitude: 4.8500 },
    { latitude: 52.3050, longitude: 4.8500 },
    { latitude: 52.3100, longitude: 4.8520 },
    { latitude: 52.3120, longitude: 4.8600 },
];

const TICK_MS = 500;
// 36 km/h = 10 m/s, x4 time scale, 0.5 s per tick -> 20 m per tick when random() = 0.5
const STEP_M = 20;

describe('VanSimulator', () => {
    let t: number;
    let sink: RecordingSink;

    const build = (route: VanRoute, router?: Router) => new VanSimulator(route, sink, {
        pingIntervalMs: TICK_MS,
        timeScale: 4,
        baseSpeedKmh: 36,
        deliveryDurationMinSec: 20,
        deliveryDurationMaxSec: 60,
        chaosEnabled: false,
        router,
        now: () => t,
        random: () => 0.5,
    });

    const step = async (sim: VanSimulator, n = 1) => {
        for (let i = 0; i < n; i++) {
            t += TICK_MS;
            await sim.tick();
        }
    };

    const runToEnd = async (sim: VanSimulator) => {
        for (let i = 0; i < 20_000 && sim.currentStatus !== 'RETURNED'; i++) await step(sim);
        expect(sim.currentStatus).toBe('RETURNED');
    };

    /** Largest distance between consecutive GPS positions. */
    const maxJump = () => sink.gps.slice(1).reduce(
        (max, e, i) => Math.max(max, haversineMeters(sink.gps[i].location, e.location)), 0);

    beforeEach(() => {
        t = 1_000_000;
        sink = new RecordingSink();
    });

    it('publishes its route plan when it starts', async () => {
        const sim = build(makeRoute(PLAIN));
        await sim.begin();
        expect(sink.plans).toHaveLength(1);
        expect(sink.plans[0]).toMatchObject({ route_id: 'route-test', version: 0, time_scale: 4, base_speed_kmh: 36 });
        expect(sink.plans[0].stops).toHaveLength(2);
        expect(sink.plans[0].waypoints[0]).toEqual([52.3, 4.85]);
    });

    it('covers speed x elapsed time x time scale per tick and reports the simulated speed', async () => {
        const sim = build(makeRoute(PLAIN));
        await sim.begin();
        await step(sim, 5);
        expect(sink.gps).toHaveLength(5);
        sink.gps.forEach((e, i) => {
            expect(e.speed_kmh).toBe(36);
            expect(haversineMeters(PLAIN[0], e.location)).toBeCloseTo(STEP_M * (i + 1), 0);
        });
    });

    it('stops exactly at the delivery location and dwells for the simulated duration', async () => {
        const sim = build(makeRoute(PLAIN));
        await sim.begin();
        const toStop = haversineMeters(PLAIN[0], PLAIN[1]);
        await step(sim, Math.ceil(toStop / STEP_M));

        expect(sim.currentStatus).toBe('DELIVERING');
        const arrival = sink.deliveries.at(-1)!;
        expect(arrival.event_type).toBe('ARRIVAL');
        expect(arrival.customer_id).toBe('cust-0');
        expect(haversineMeters(sink.gps.at(-1)!.location, PLAIN[1])).toBeLessThan(0.01);
        expect(sink.gps.at(-1)!.speed_kmh).toBe(0);

        // 40 simulated seconds / time scale 4 = 10 s = 20 ticks
        await step(sim, 19);
        expect(sim.currentStatus).toBe('DELIVERING');
        await step(sim, 1);
        expect(sim.currentStatus).toBe('EN_ROUTE');
        const [done, departure] = sink.deliveries.slice(-2);
        expect(done.event_type).toBe('DELIVERY_COMPLETED');
        expect(done.delivery_duration_seconds).toBe(40);
        expect(done.parcels_delivered).toBe(2);
        expect(departure.event_type).toBe('DEPARTURE');
        expect(sink.gps.at(-1)!.current_stop_index).toBe(1);
    });

    it('visits every stop in order, never jumps, and finishes at the return point', async () => {
        let completed = '';
        const route = makeRoute(PLAIN);
        const sim = new VanSimulator(route, sink, {
            pingIntervalMs: TICK_MS, timeScale: 4, baseSpeedKmh: 36, chaosEnabled: false,
            now: () => t, random: () => 0.5, onRouteCompleted: (id) => { completed = id; },
        });
        await sim.begin();
        await runToEnd(sim);

        expect(completed).toBe('van-test');
        expect(sink.deliveries.map((d) => `${d.stop_index}:${d.event_type}`)).toEqual([
            '0:ARRIVAL', '0:DELIVERY_COMPLETED', '0:DEPARTURE',
            '1:ARRIVAL', '1:DELIVERY_COMPLETED', '1:DEPARTURE',
        ]);
        expect(maxJump()).toBeLessThanOrEqual(STEP_M + 0.01);
        const last = sink.gps.at(-1)!;
        expect(last.status).toBe('RETURNED');
        expect(haversineMeters(last.location, PLAIN[3])).toBeLessThan(0.01);
    });

    it('slows down inside delay zones', async () => {
        const zone = DELAY_ZONES.find((z) => z.name === 'De Pijp School Zone')!;
        const route = makeRoute([
            { latitude: 52.3490, longitude: 4.8940 },
            { latitude: 52.3560, longitude: 4.8940 },
            { latitude: 52.3570, longitude: 4.8940 },
        ]);
        const sim = build(route);
        await sim.begin();
        await step(sim, 40);
        const moving = sink.gps.filter((e) => e.speed_kmh > 0);
        const inside = moving.filter((e) => pointInRing(e.location, zone.ring));
        const outside = moving.filter((e) => !pointInRing(e.location, zone.ring));
        expect(inside.length).toBeGreaterThan(5);
        expect(outside.length).toBeGreaterThan(2);
        // The speed of a tick is decided at its starting point, so allow one tick of overlap at the edges.
        expect(inside.filter((e) => e.speed_kmh === 21.6).length).toBeGreaterThan(inside.length - 2);
        expect(outside.filter((e) => e.speed_kmh === 36).length).toBeGreaterThan(outside.length - 2);
    });

    it('with chaos on, still delivers every ping and sends RETURNED last', async () => {
        const sim = new VanSimulator(makeRoute(PLAIN), sink, {
            pingIntervalMs: TICK_MS, timeScale: 4, baseSpeedKmh: 36, chaosEnabled: true,
            now: () => t, random: () => 0.5,
            // Deliver immediately; retries on every ping; no dead zones
            schedule: (fn) => fn(),
            link: { deadZoneProbability: 0, duplicates: { duplicateProbability: 1, maxDuplicates: 1 } },
        });
        await sim.begin();
        await runToEnd(sim);
        await sim.stop();

        const unique = new Set(sink.gps.map((e) => e.sequence_number));
        expect(sink.gps.length).toBe(2 * unique.size - 1); // every ping twice except the final one
        expect(sink.gps.at(-1)!.status).toBe('RETURNED');
        expect(sink.gps.filter((e) => e.status === 'RETURNED')).toHaveLength(1);
    });

    describe('insertStop', () => {
        const side: Waypoint = { latitude: 52.3030, longitude: 4.8540 };

        it('reroutes to a stop inserted before the next one, without backtracking', async () => {
            const sim = build(makeRoute(PLAIN), new StraightLineRouter());
            await sim.begin();
            await step(sim, 3);

            const index = await sim.insertStop({
                beforeIndex: 0, location: side, slaDeadline: new Date(t + 600_000), customerId: 'adhoc',
            });
            expect(index).toBe(0);
            expect(sim.stops.map((s) => s.customer_id)).toEqual(['adhoc', 'cust-0', 'cust-1']);
            expect(sim.stops.map((s) => s.stop_index)).toEqual([0, 1, 2]);

            const plan = sink.plans.at(-1)!;
            expect(plan.version).toBe(1);
            const vertex = plan.waypoints[plan.stops[0].waypoint_index];
            expect(haversineMeters({ latitude: vertex[0], longitude: vertex[1] }, side)).toBeLessThan(0.01);

            await runToEnd(sim);
            expect(sink.deliveries.filter((d) => d.event_type === 'ARRIVAL').map((d) => d.customer_id))
                .toEqual(['adhoc', 'cust-0', 'cust-1']);
            expect(maxJump()).toBeLessThanOrEqual(STEP_M + 0.01);
        });

        it('inserts after the stop being served when the van is delivering', async () => {
            const sim = build(makeRoute(PLAIN), new StraightLineRouter());
            await sim.begin();
            while (sim.currentStatus !== 'DELIVERING') await step(sim);

            const index = await sim.insertStop({
                beforeIndex: 0, location: side, slaDeadline: new Date(t + 600_000), customerId: 'adhoc',
            });
            expect(index).toBe(1);
            await runToEnd(sim);
            expect(sink.deliveries.filter((d) => d.event_type === 'ARRIVAL').map((d) => d.customer_id))
                .toEqual(['cust-0', 'adhoc', 'cust-1']);
            expect(maxJump()).toBeLessThanOrEqual(STEP_M + 0.01);
        });

        it('moves the stop after the next one when the van is too close to divert', async () => {
            const sim = build(makeRoute(PLAIN), new StraightLineRouter());
            await sim.begin();
            const toStop = haversineMeters(PLAIN[0], PLAIN[1]);
            await step(sim, Math.floor((toStop - 100) / STEP_M)); // ~100 m before the stop

            const index = await sim.insertStop({
                beforeIndex: 0, location: side, slaDeadline: new Date(t + 600_000), customerId: 'adhoc',
            });
            expect(index).toBe(1);
            await runToEnd(sim);
            expect(sink.deliveries.filter((d) => d.event_type === 'ARRIVAL').map((d) => d.customer_id))
                .toEqual(['cust-0', 'adhoc', 'cust-1']);
            expect(maxJump()).toBeLessThanOrEqual(STEP_M + 0.01);
        });

        it('retries when the van drives past the detour start while the router is busy', async () => {
            let calls = 0;
            let sim!: VanSimulator;
            const slowRouter: Router = {
                async route(points: Waypoint[]): Promise<RoutedPath> {
                    calls++;
                    if (calls === 1) await step(sim, 15); // 300 m further by the time the answer arrives
                    return straightLinePath(points);
                },
            };
            sim = build(makeRoute(PLAIN), slowRouter);
            await sim.begin();
            await step(sim, 2);

            const index = await sim.insertStop({
                beforeIndex: 0, location: side, slaDeadline: new Date(t + 600_000), customerId: 'adhoc',
            });
            expect(calls).toBe(2);
            expect(index).toBe(0);
            await runToEnd(sim);
            expect(maxJump()).toBeLessThanOrEqual(STEP_M + 0.01);
        });

        it('rejects stops once the route is finished', async () => {
            const sim = build(makeRoute(PLAIN), new StraightLineRouter());
            await sim.begin();
            await runToEnd(sim);
            expect(await sim.insertStop({
                beforeIndex: 0, location: side, slaDeadline: new Date(t), customerId: 'late',
            })).toBeNull();
        });
    });
});
