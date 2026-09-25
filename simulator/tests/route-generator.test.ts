import { describe, it, expect } from 'vitest';
import {
    generateRoute, haversineDistance, calculateBearing, matchLegEnds, straightLinePath,
    OsrmRouter, StraightLineRouter, HUBS,
} from '../src/route-generator.js';
import { cumulativeDistances, densify } from '../src/geo.js';
import { type Waypoint } from '../src/models/index.js';

const OPTS = { router: new StraightLineRouter(), timeScale: 4, baseSpeedKmh: 25 };

/** Deterministic PRNG so failures are reproducible. */
function seeded(seed: number): () => number {
    let s = seed >>> 0;
    return () => {
        s = (s * 1664525 + 1013904223) >>> 0;
        return s / 2 ** 32;
    };
}

describe('generateRoute', () => {
    it('creates the requested number of stops with unique customers', async () => {
        const route = await generateRoute(0, 15, OPTS);
        expect(route.stops).toHaveLength(15);
        expect(route.van_id).toBe('van-000');
        expect(route.route_id).toContain('van-000');
        expect(new Set(route.stops.map((s) => s.customer_id)).size).toBe(15);
    });

    it('keeps stops inside the Amsterdam area', async () => {
        const route = await generateRoute(3, 20, OPTS);
        for (const stop of route.stops) {
            expect(stop.location.latitude).toBeGreaterThan(52.28);
            expect(stop.location.latitude).toBeLessThan(52.42);
            expect(stop.location.longitude).toBeGreaterThan(4.82);
            expect(stop.location.longitude).toBeLessThan(4.98);
        }
    });

    it('starts and ends near the van\'s hub', async () => {
        for (const vanIndex of [0, 1, 2, 3]) {
            const route = await generateRoute(vanIndex, 5, OPTS);
            const hub = HUBS[vanIndex % HUBS.length];
            // Scatter radius is 0.015° (~1.7 km)
            expect(haversineDistance(route.waypoints[0], hub)).toBeLessThan(1.8);
            expect(haversineDistance(route.waypoints[route.waypoints.length - 1], hub)).toBeLessThan(1.8);
        }
    });

    it('places each stop exactly on its route vertex, in increasing order', async () => {
        const route = await generateRoute(1, 18, { ...OPTS, random: seeded(7) });
        let prev = 0;
        for (const stop of route.stops) {
            expect(stop.waypoint_index).toBeGreaterThanOrEqual(prev);
            const vertex = route.waypoints[stop.waypoint_index];
            expect(vertex.latitude).toBeCloseTo(stop.location.latitude, 9);
            expect(vertex.longitude).toBeCloseTo(stop.location.longitude, 9);
            prev = stop.waypoint_index;
        }
    });

    it('plans arrivals from driving distance, speed, dwell time and time scale', async () => {
        const now = new Date('2026-09-25T10:00:00Z');
        const route = await generateRoute(2, 10, { ...OPTS, now, expectedDwellSec: 40, random: seeded(3) });
        const cum = cumulativeDistances(route.waypoints);
        let expectedSec = 0;
        let prevVertex = 0;
        route.stops.forEach((stop, i) => {
            expectedSec += (cum[stop.waypoint_index] - cum[prevVertex]) / (25 / 3.6) + (i > 0 ? 40 : 0);
            prevVertex = stop.waypoint_index;
            const planned = new Date(stop.planned_arrival).getTime();
            expect(planned).toBeCloseTo(now.getTime() + (expectedSec / 4) * 1000, -1);
        });
    });

    it('gives most stops 3-10 simulated minutes of slack and some a tight slot', async () => {
        let tight = 0;
        let total = 0;
        for (let v = 0; v < 20; v++) {
            const route = await generateRoute(v, 18, { ...OPTS, random: seeded(100 + v) });
            for (const stop of route.stops) {
                const slackSimSec = (Date.parse(stop.sla_deadline) - Date.parse(stop.planned_arrival)) / 1000 * 4;
                total++;
                if (slackSimSec < 180) {
                    tight++;
                    expect(slackSimSec).toBeGreaterThanOrEqual(-60.01);
                    expect(slackSimSec).toBeLessThanOrEqual(90.01);
                } else {
                    expect(slackSimSec).toBeLessThanOrEqual(600.01);
                }
            }
        }
        expect(tight / total).toBeGreaterThan(0.08);
        expect(tight / total).toBeLessThan(0.25);
    });

    it('varies stop counts via its argument and gives each van its own id', async () => {
        const a = await generateRoute(4, 14, OPTS);
        const b = await generateRoute(5, 22, OPTS);
        expect(a.stops).toHaveLength(14);
        expect(b.stops).toHaveLength(22);
        expect(a.van_id).not.toBe(b.van_id);
    });
});

describe('routers', () => {
    const points: Waypoint[] = [
        { latitude: 52.35, longitude: 4.90 },
        { latitude: 52.36, longitude: 4.91 },
        { latitude: 52.37, longitude: 4.89 },
    ];

    it('straight-line path ends each leg on the input point', () => {
        const path = straightLinePath(points);
        expect(path.legEndIndices).toHaveLength(2);
        expect(path.waypoints[path.legEndIndices[0]]).toEqual(points[1]);
        expect(path.legEndIndices[1]).toBe(path.waypoints.length - 1);
    });

    it('OSRM router maps legs to vertices using leg distances and snapped points', async () => {
        // Fake OSRM: geometry is a straight line with the middle point snapped 3 m away.
        const leg1 = densify(points[0], points[1], 40);
        const leg2 = densify(points[1], points[2], 40);
        const coords = [...leg1, ...leg2.slice(1)];
        const cum = cumulativeDistances(coords);
        const fakeFetch = async () => ({
            ok: true,
            status: 200,
            json: async () => ({
                routes: [{
                    geometry: { coordinates: coords.map((p) => [p.longitude, p.latitude]) },
                    legs: [{ distance: cum[leg1.length - 1] + 2 }, { distance: cum[cum.length - 1] - cum[leg1.length - 1] - 2 }],
                }],
                waypoints: [points[0], { latitude: 52.36002, longitude: 4.91 }, points[2]]
                    .map((p) => ({ location: [p.longitude, p.latitude] })),
            }),
        });
        const router = new OsrmRouter('http://osrm.test', fakeFetch);
        const path = await router.route(points);
        expect(path.source).toBe('osrm');
        expect(path.legEndIndices).toEqual([leg1.length - 1, coords.length - 1]);
    });

    it('OSRM router falls back to straight lines on HTTP errors', async () => {
        const router = new OsrmRouter('http://osrm.test', async () => ({ ok: false, status: 429, json: async () => ({}) }));
        const path = await router.route(points);
        expect(path.source).toBe('straight-line');
    });

    it('OSRM router falls back to straight lines when unreachable', async () => {
        const router = new OsrmRouter('http://osrm.test', async () => { throw new Error('ECONNREFUSED'); });
        expect((await router.route(points)).source).toBe('straight-line');
    });

    it('leg ends never go backwards even if distances are inconsistent', () => {
        const line = densify(points[0], points[2], 30);
        const ends = matchLegEnds(line, [5000, 1, 1, 5000]);
        for (let i = 1; i < ends.length; i++) expect(ends[i]).toBeGreaterThanOrEqual(ends[i - 1]);
        expect(ends[ends.length - 1]).toBe(line.length - 1);
    });
});

describe('haversineDistance', () => {
    it('is about 0.8 km from Centraal to Dam', () => {
        const dist = haversineDistance(
            { latitude: 52.3791, longitude: 4.9003 },
            { latitude: 52.3730, longitude: 4.8932 },
        );
        expect(dist).toBeGreaterThan(0.5);
        expect(dist).toBeLessThan(1.5);
    });

    it('is 0 for the same point', () => {
        const point = { latitude: 52.37, longitude: 4.90 };
        expect(haversineDistance(point, point)).toBe(0);
    });
});

describe('calculateBearing', () => {
    it('is ~0 due north', () => {
        expect(calculateBearing({ latitude: 52.37, longitude: 4.90 }, { latitude: 52.38, longitude: 4.90 })).toBeCloseTo(0, 0);
    });

    it('is ~90 due east', () => {
        expect(calculateBearing({ latitude: 52.37, longitude: 4.90 }, { latitude: 52.37, longitude: 4.92 })).toBeCloseTo(90, 0);
    });

    it('stays within [0, 360)', () => {
        const random = seeded(11);
        for (let i = 0; i < 20; i++) {
            const bearing = calculateBearing(
                { latitude: 52.37 + random() * 0.1, longitude: 4.90 + random() * 0.1 },
                { latitude: 52.37 + random() * 0.1, longitude: 4.90 + random() * 0.1 },
            );
            expect(bearing).toBeGreaterThanOrEqual(0);
            expect(bearing).toBeLessThan(360);
        }
    });
});
