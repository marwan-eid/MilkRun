import { type Waypoint } from './models/index.js';

const EARTH_RADIUS_M = 6_371_000;
const toRad = (deg: number) => (deg * Math.PI) / 180;

/** Great-circle distance in kilometres. */
export function haversineDistance(a: Waypoint, b: Waypoint): number {
    return haversineMeters(a, b) / 1000;
}

/** Great-circle distance in metres. */
export function haversineMeters(a: Waypoint, b: Waypoint): number {
    const dLat = toRad(b.latitude - a.latitude);
    const dLon = toRad(b.longitude - a.longitude);
    const h = Math.sin(dLat / 2) ** 2 +
        Math.cos(toRad(a.latitude)) * Math.cos(toRad(b.latitude)) * Math.sin(dLon / 2) ** 2;
    return 2 * EARTH_RADIUS_M * Math.asin(Math.min(1, Math.sqrt(h)));
}

/** Compass bearing from a to b in degrees [0, 360). */
export function calculateBearing(from: Waypoint, to: Waypoint): number {
    const dLon = toRad(to.longitude - from.longitude);
    const lat1 = toRad(from.latitude);
    const lat2 = toRad(to.latitude);
    const y = Math.sin(dLon) * Math.cos(lat2);
    const x = Math.cos(lat1) * Math.sin(lat2) - Math.sin(lat1) * Math.cos(lat2) * Math.cos(dLon);
    return ((Math.atan2(y, x) * 180) / Math.PI + 360) % 360;
}

/** Linear interpolation between two points (fine at city scale). */
export function lerp(a: Waypoint, b: Waypoint, t: number): Waypoint {
    return {
        latitude: a.latitude + (b.latitude - a.latitude) * t,
        longitude: a.longitude + (b.longitude - a.longitude) * t,
    };
}

/** Cumulative distance in metres at each vertex of a polyline (first entry is 0). */
export function cumulativeDistances(points: Waypoint[]): number[] {
    const cum = new Array<number>(points.length);
    cum[0] = 0;
    for (let i = 1; i < points.length; i++) {
        cum[i] = cum[i - 1] + haversineMeters(points[i - 1], points[i]);
    }
    return cum;
}

/** Straight-line polyline between points with a vertex every ~stepMeters. */
export function densify(from: Waypoint, to: Waypoint, stepMeters = 50): Waypoint[] {
    const n = Math.max(1, Math.ceil(haversineMeters(from, to) / stepMeters));
    const out: Waypoint[] = [];
    for (let i = 0; i <= n; i++) {
        out.push(lerp(from, to, i / n));
    }
    return out;
}

// ═══════════════════════════════════════════════════════════
// Delay zones
// ═══════════════════════════════════════════════════════════

export interface DelayZone {
    name: string;
    speedFactor: number;
    /** Polygon ring as [longitude, latitude] pairs (GeoJSON order). */
    ring: [number, number][];
}

/**
 * Mirrors the seed rows of geofence_zones in infra/postgres/init.sql (and the
 * backend's Flyway migration). The simulator slows vans inside these zones; the
 * backend reads the same polygons from PostGIS to predict the slowdown.
 */
export const DELAY_ZONES: DelayZone[] = [
    rect('Vondelpark Construction', 0.40, 4.8650, 52.3580, 4.8750, 52.3620),
    rect('Central Station Traffic', 0.50, 4.8950, 52.3770, 4.9050, 52.3810),
    rect('De Pijp School Zone', 0.60, 4.8900, 52.3500, 4.8980, 52.3540),
    rect('Jordaan Narrow Streets', 0.55, 4.8780, 52.3700, 4.8870, 52.3740),
];

function rect(name: string, speedFactor: number, minLon: number, minLat: number, maxLon: number, maxLat: number): DelayZone {
    return {
        name, speedFactor,
        ring: [[minLon, minLat], [maxLon, minLat], [maxLon, maxLat], [minLon, maxLat], [minLon, minLat]],
    };
}

/** Ray-casting point-in-polygon test. */
export function pointInRing(p: Waypoint, ring: [number, number][]): boolean {
    let inside = false;
    for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
        const [xi, yi] = ring[i];
        const [xj, yj] = ring[j];
        const crosses = (yi > p.latitude) !== (yj > p.latitude) &&
            p.longitude < ((xj - xi) * (p.latitude - yi)) / (yj - yi) + xi;
        if (crosses) inside = !inside;
    }
    return inside;
}

/** The zone containing the point, or null. */
export function zoneAt(p: Waypoint, zones: DelayZone[] = DELAY_ZONES): DelayZone | null {
    for (const z of zones) {
        if (pointInRing(p, z.ring)) return z;
    }
    return null;
}

/** Speed multiplier at a point: the zone's factor, or 1 outside all zones. */
export function speedFactorAt(p: Waypoint, zones: DelayZone[] = DELAY_ZONES): number {
    return zoneAt(p, zones)?.speedFactor ?? 1;
}
