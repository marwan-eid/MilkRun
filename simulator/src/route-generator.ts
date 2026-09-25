import { type Waypoint, type RouteStop, type VanRoute } from './models/index.js';
import { cumulativeDistances, densify, haversineMeters } from './geo.js';

export { haversineDistance, calculateBearing } from './geo.js';

// ═══════════════════════════════════════════════════════════
// Amsterdam delivery area
// ═══════════════════════════════════════════════════════════

/** Micro-hubs. Van i starts and ends near hub i % 4, so vans spread across the city. */
export const HUBS: Waypoint[] = [
    { latitude: 52.3548, longitude: 4.9578 }, // East (Science Park)
    { latitude: 52.3950, longitude: 4.8970 }, // North (NDSM Wharf area)
    { latitude: 52.3420, longitude: 4.8700 }, // South (Zuidas District)
    { latitude: 52.3700, longitude: 4.8350 }, // West (Rembrandtpark area)
];

/**
 * Delivery neighborhoods. Stops are scattered uniformly inside `radius`
 * degrees (~250-900 m) of each center.
 */
export const NEIGHBORHOODS = [
    { name: 'De Pijp', center: { latitude: 52.3520, longitude: 4.8930 }, radius: 0.005 },
    { name: 'Jordaan', center: { latitude: 52.3740, longitude: 4.8830 }, radius: 0.004 },
    { name: 'Oud-West', center: { latitude: 52.3650, longitude: 4.8700 }, radius: 0.005 },
    { name: 'Oost', center: { latitude: 52.3610, longitude: 4.9280 }, radius: 0.006 },
    { name: 'Noord', center: { latitude: 52.3900, longitude: 4.9200 }, radius: 0.007 },
    { name: 'Centrum', center: { latitude: 52.3700, longitude: 4.8950 }, radius: 0.004 },
    { name: 'Amstelveen', center: { latitude: 52.3020, longitude: 4.8500 }, radius: 0.008 },
    { name: 'Buitenveldert', center: { latitude: 52.3300, longitude: 4.8770 }, radius: 0.005 },
    { name: 'Watergraafsmeer', center: { latitude: 52.3530, longitude: 4.9350 }, radius: 0.004 },
    { name: 'Rivierenbuurt', center: { latitude: 52.3450, longitude: 4.9050 }, radius: 0.004 },
];

// ═══════════════════════════════════════════════════════════
// Routing
// ═══════════════════════════════════════════════════════════

/** A drivable path through a list of points. */
export interface RoutedPath {
    waypoints: Waypoint[];
    /** legEndIndices[i] is the waypoint index where the path reaches points[i + 1]. */
    legEndIndices: number[];
    source: 'osrm' | 'straight-line';
}

export interface Router {
    route(points: Waypoint[]): Promise<RoutedPath>;
}

/** Straight segments with a vertex every ~50 m. Used offline and as the OSRM fallback. */
export class StraightLineRouter implements Router {
    async route(points: Waypoint[]): Promise<RoutedPath> {
        return straightLinePath(points);
    }
}

export function straightLinePath(points: Waypoint[]): RoutedPath {
    const waypoints: Waypoint[] = [points[0]];
    const legEndIndices: number[] = [];
    for (let i = 0; i < points.length - 1; i++) {
        waypoints.push(...densify(points[i], points[i + 1]).slice(1));
        legEndIndices.push(waypoints.length - 1);
    }
    return { waypoints, legEndIndices, source: 'straight-line' };
}

type FetchFn = (url: string, init?: { signal?: AbortSignal }) => Promise<{
    ok: boolean;
    status: number;
    json(): Promise<any>;
}>;

/**
 * Street geometry from an OSRM server. Falls back to straight lines when the
 * server is unreachable or rate-limits us (the public demo server allows about
 * one request per second).
 */
export class OsrmRouter implements Router {
    constructor(
        private readonly baseUrl: string = process.env.OSRM_URL || 'https://router.project-osrm.org',
        private readonly fetchImpl: FetchFn = fetch,
        private readonly timeoutMs = 10_000,
    ) { }

    async route(points: Waypoint[]): Promise<RoutedPath> {
        const coords = points.map((p) => `${p.longitude.toFixed(6)},${p.latitude.toFixed(6)}`).join(';');
        const url = `${this.baseUrl}/route/v1/driving/${coords}?overview=full&geometries=geojson`;
        try {
            const response = await this.fetchImpl(url, { signal: AbortSignal.timeout(this.timeoutMs) });
            if (!response.ok) {
                console.warn(`OSRM returned HTTP ${response.status}; using straight-line geometry.`);
                return straightLinePath(points);
            }
            const data = await response.json();
            const route = data?.routes?.[0];
            if (!route || !Array.isArray(route.legs) || route.legs.length !== points.length - 1) {
                return straightLinePath(points);
            }
            const waypoints: Waypoint[] = route.geometry.coordinates.map((c: number[]) => ({
                longitude: c[0],
                latitude: c[1],
            }));
            if (waypoints.length < 2) {
                return straightLinePath(points);
            }
            const snapped: Waypoint[] | undefined = Array.isArray(data.waypoints) && data.waypoints.length === points.length
                ? data.waypoints.map((w: { location: number[] }) => ({ longitude: w.location[0], latitude: w.location[1] }))
                : undefined;
            const legEndIndices = matchLegEnds(waypoints, route.legs.map((l: { distance: number }) => l.distance), snapped);
            return { waypoints, legEndIndices, source: 'osrm' };
        } catch (e) {
            console.warn(`OSRM unreachable (${(e as Error).message}); using straight-line geometry.`);
            return straightLinePath(points);
        }
    }
}

/**
 * Finds the polyline vertex where each OSRM leg ends. The leg distances say how
 * far along the geometry each leg ends; within a few vertices of that distance
 * we take the vertex closest to the snapped waypoint. Indices never decrease,
 * and the last leg always ends at the final vertex.
 */
export function matchLegEnds(waypoints: Waypoint[], legDistances: number[], snapped?: Waypoint[]): number[] {
    const cum = cumulativeDistances(waypoints);
    const last = waypoints.length - 1;
    const scale = legDistances.reduce((a, b) => a + b, 0) > 0
        ? cum[last] / legDistances.reduce((a, b) => a + b, 0)
        : 1;
    const ends: number[] = [];
    let target = 0;
    let prev = 0;
    for (let i = 0; i < legDistances.length; i++) {
        if (i === legDistances.length - 1) {
            ends.push(last);
            break;
        }
        target += legDistances[i] * scale;
        let idx = prev;
        while (idx < last && cum[idx + 1] <= target) idx++;
        if (idx < last && Math.abs(cum[idx + 1] - target) < Math.abs(cum[idx] - target)) idx++;
        const point = snapped?.[i + 1];
        if (point) {
            let best = idx;
            let bestDist = haversineMeters(waypoints[idx], point);
            for (let j = Math.max(prev, idx - 15); j <= Math.min(last, idx + 15); j++) {
                const d = haversineMeters(waypoints[j], point);
                if (d < bestDist) {
                    best = j;
                    bestDist = d;
                }
            }
            idx = best;
        }
        idx = Math.max(idx, prev);
        ends.push(idx);
        prev = idx;
    }
    return ends;
}

// ═══════════════════════════════════════════════════════════
// Route construction
// ═══════════════════════════════════════════════════════════

export interface RouteOptions {
    router: Router;
    /** Simulated seconds per wall-clock second. */
    timeScale: number;
    /** Free-flow driving speed in simulated km/h, used to plan arrival times. */
    baseSpeedKmh: number;
    /** Expected time spent at each stop, simulated seconds. */
    expectedDwellSec?: number;
    /** Share of stops whose delivery slot leaves almost no slack. */
    tightSlotShare?: number;
    now?: Date;
    random?: () => number;
}

/** Uniform random point in a circle of `radius` degrees. */
function scatterPoint(center: Waypoint, radius: number, random: () => number): Waypoint {
    const angle = random() * 2 * Math.PI;
    const r = radius * Math.sqrt(random());
    return {
        latitude: center.latitude + r * Math.cos(angle),
        longitude: center.longitude + r * Math.sin(angle),
    };
}

/** Greedy nearest-neighbour ordering starting from `from`. */
function nearestNeighbourOrder<T>(from: Waypoint, items: T[], locate: (t: T) => Waypoint): T[] {
    const remaining = [...items];
    const ordered: T[] = [];
    let current = from;
    while (remaining.length > 0) {
        let bestIdx = 0;
        let best = Infinity;
        remaining.forEach((item, i) => {
            const d = haversineMeters(current, locate(item));
            if (d < best) {
                best = d;
                bestIdx = i;
            }
        });
        const [next] = remaining.splice(bestIdx, 1);
        ordered.push(next);
        current = locate(next);
    }
    return ordered;
}

/**
 * Simulated seconds to drive from vertex `from` to vertex `to` at `speedKmh`.
 */
export function travelSeconds(cum: number[], from: number, to: number, speedKmh: number): number {
    return Math.max(0, cum[to] - cum[from]) / (speedKmh / 3.6);
}

/**
 * Builds a van's route: 2-4 neighborhoods visited in nearest-neighbour order,
 * stops ordered nearest-neighbour inside each, street geometry from the router,
 * and a delivery slot per stop.
 *
 * Planned arrivals assume free-flow speed and the expected dwell time. Most
 * slots leave 3-10 simulated minutes of slack; `tightSlotShare` of them leave
 * between -1 and +1.5 minutes, so traffic in the delay zones or an inserted
 * ad-hoc stop can push them past their deadline.
 */
export async function generateRoute(vanIndex: number, totalStops: number, opts: RouteOptions): Promise<VanRoute> {
    const random = opts.random ?? Math.random;
    const now = opts.now ?? new Date();
    const expectedDwellSec = opts.expectedDwellSec ?? 40;
    const tightSlotShare = opts.tightSlotShare ?? 0.15;

    const vanId = `van-${String(vanIndex).padStart(3, '0')}`;
    const routeId = `route-${now.toISOString().slice(0, 10)}-${vanId}-${Math.floor(now.getTime() / 1000)}`;

    // Pick 2-4 random neighborhoods (unbiased Fisher-Yates shuffle)
    const shuffled = [...NEIGHBORHOODS];
    for (let j = shuffled.length - 1; j > 0; j--) {
        const r = Math.floor(random() * (j + 1));
        [shuffled[j], shuffled[r]] = [shuffled[r], shuffled[j]];
    }
    const chosen = shuffled.slice(0, 2 + Math.floor(random() * 3));

    const hub = HUBS[vanIndex % HUBS.length];
    const origin = scatterPoint(hub, 0.015, random);
    const destination = scatterPoint(hub, 0.015, random);

    // Scatter stops, then order: neighborhoods nearest-first, stops nearest-first within each
    const byNeighborhood = chosen.map((n, i) => ({
        center: n.center,
        stops: Array.from({ length: Math.floor(totalStops / chosen.length) + (i < totalStops % chosen.length ? 1 : 0) },
            () => scatterPoint(n.center, n.radius, random)),
    }));
    const locations: Waypoint[] = [];
    let cursor = origin;
    for (const n of nearestNeighbourOrder(origin, byNeighborhood, (x) => x.center)) {
        const ordered = nearestNeighbourOrder(cursor, n.stops, (p) => p);
        locations.push(...ordered);
        if (ordered.length > 0) cursor = ordered[ordered.length - 1];
    }

    const path = await opts.router.route([origin, ...locations, destination]);
    const cum = cumulativeDistances(path.waypoints);

    const stops: RouteStop[] = [];
    let plannedSec = 0;
    let prevVertex = 0;
    locations.forEach((location, i) => {
        const vertex = path.legEndIndices[i];
        plannedSec += travelSeconds(cum, prevVertex, vertex, opts.baseSpeedKmh) + (i > 0 ? expectedDwellSec : 0);
        prevVertex = vertex;
        const slackSec = random() < tightSlotShare
            ? -60 + random() * 150
            : 180 + random() * 420;
        const plannedArrival = new Date(now.getTime() + (plannedSec / opts.timeScale) * 1000);
        stops.push({
            stop_index: i,
            customer_id: `cust-${String(vanIndex).padStart(3, '0')}-${String(i).padStart(2, '0')}`,
            location,
            planned_arrival: plannedArrival.toISOString(),
            sla_deadline: new Date(plannedArrival.getTime() + (slackSec / opts.timeScale) * 1000).toISOString(),
            parcels: 1 + Math.floor(random() * 5),
            waypoint_index: vertex,
        });
    });

    return {
        route_id: routeId,
        van_id: vanId,
        created_at: now.toISOString(),
        stops,
        waypoints: path.waypoints,
    };
}
