import { type Waypoint, type RouteStop, type VanRoute } from './models/index.js';

// ═══════════════════════════════════════════════════════════
// Amsterdam area delivery neighborhoods with realistic coordinates
// ═══════════════════════════════════════════════════════════

/** 
 * Decentralized Micro-Hubs (Vans start and end here based on assigned quadrant)
 * This disperses the vans across Amsterdam rather than chaining them from a single endpoint.
 */
const HUBS: Waypoint[] = [
    { latitude: 52.3548, longitude: 4.9578 }, // East (Original Science Park)
    { latitude: 52.3950, longitude: 4.8970 }, // North (NDSM Wharf area)
    { latitude: 52.3420, longitude: 4.8700 }, // South (Zuidas District)
    { latitude: 52.3700, longitude: 4.8350 }, // West (Rembrandtpark area)
];

/**
 * Delivery neighborhoods around Amsterdam.
 * Each neighborhood has a center and a radius (in degrees ≈ ~100-300m)
 * that we scatter delivery stops around.
 */
const NEIGHBORHOODS = [
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

/**
 * Generate a random point near a center within a given radius.
 */
function scatterPoint(center: Waypoint, radius: number): Waypoint {
    const angle = Math.random() * 2 * Math.PI;
    const r = radius * Math.sqrt(Math.random()); // uniform distribution in circle
    return {
        latitude: center.latitude + r * Math.cos(angle),
        longitude: center.longitude + r * Math.sin(angle),
    };
}

/**
 * Calculate compass bearing from point A to B (in degrees).
 */
export function calculateBearing(from: Waypoint, to: Waypoint): number {
    const dLon = ((to.longitude - from.longitude) * Math.PI) / 180;
    const lat1 = (from.latitude * Math.PI) / 180;
    const lat2 = (to.latitude * Math.PI) / 180;
    const y = Math.sin(dLon) * Math.cos(lat2);
    const x = Math.cos(lat1) * Math.sin(lat2) - Math.sin(lat1) * Math.cos(lat2) * Math.cos(dLon);
    return ((Math.atan2(y, x) * 180) / Math.PI + 360) % 360;
}

/**
 * Haversine distance between two points in km.
 */
export function haversineDistance(a: Waypoint, b: Waypoint): number {
    const R = 6371; // Earth radius in km
    const dLat = ((b.latitude - a.latitude) * Math.PI) / 180;
    const dLon = ((b.longitude - a.longitude) * Math.PI) / 180;
    const lat1 = (a.latitude * Math.PI) / 180;
    const lat2 = (b.latitude * Math.PI) / 180;
    const h = Math.sin(dLat / 2) ** 2 + Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) ** 2;
    return 2 * R * Math.asin(Math.sqrt(h));
}

/**
 * Interpolate dense waypoints between two locations.
 * Creates points roughly every ~50 meters for smooth animation.
 */
function interpolateWaypoints(from: Waypoint, to: Waypoint): Waypoint[] {
    const dist = haversineDistance(from, to);
    const numPoints = Math.max(2, Math.ceil(dist / 0.05)); // ~50m intervals
    const waypoints: Waypoint[] = [];

    for (let i = 0; i <= numPoints; i++) {
        const t = i / numPoints;
        waypoints.push({
            latitude: from.latitude + t * (to.latitude - from.latitude),
            longitude: from.longitude + t * (to.longitude - from.longitude),
        });
    }

    return waypoints;
}

/**
 * Fetch a completely realistic polyline from the Open Source Routing Machine
 * projecting the driving route over physical street geometry.
 */
export async function fetchOsrmRoute(points: Waypoint[]): Promise<Waypoint[]> {
    const coords = points.map(p => `${p.longitude.toFixed(6)},${p.latitude.toFixed(6)}`).join(';');
    const url = `http://router.project-osrm.org/route/v1/driving/${coords}?overview=full&geometries=geojson`;

    try {
        const response = await fetch(url);
        if (!response.ok) {
            console.warn(`OSRM API Error: ${response.status} - Falling back to Cartesian geometry.`);
            return fallbackOsrmRoute(points);
        }

        const data = await response.json();
        if (!data.routes || data.routes.length === 0) {
            return fallbackOsrmRoute(points);
        }

        const geo = data.routes[0].geometry.coordinates; // [ [lon, lat], ... ]
        return geo.map((c: number[]) => ({
            longitude: c[0],
            latitude: c[1]
        }));
    } catch (e) {
        console.warn(`OSRM API Offline: Falling back to Cartesian geometry.`);
        return fallbackOsrmRoute(points);
    }
}

/** Fallback method routing points together linearly if OSRM rejects the HTTP payload (e.g. Rate Limit IP Ban) */
function fallbackOsrmRoute(points: Waypoint[]): Waypoint[] {
    const waypoints: Waypoint[] = [];
    for (let i = 0; i < points.length - 1; i++) {
        const segment = interpolateWaypoints(points[i], points[i + 1]);
        waypoints.push(...(i === 0 ? segment : segment.slice(1)));
    }
    return waypoints;
}

/**
 * Generate a unique customer ID for a stop.
 */
function generateCustomerId(vanIndex: number, stopIndex: number): string {
    return `cust-${String(vanIndex).padStart(3, '0')}-${String(stopIndex).padStart(2, '0')}`;
}

/**
 * Generate a single van's route with realistic Amsterdam stops.
 */
export async function generateRoute(vanIndex: number, totalStops: number): Promise<VanRoute> {
    const today = new Date().toISOString().slice(0, 10);
    const vanId = `van-${String(vanIndex).padStart(3, '0')}`;
    const routeId = `route-${today}-${vanId}`;

    // Pick 2-4 random neighborhoods for this van to service
    const shuffled = [...NEIGHBORHOODS].sort(() => Math.random() - 0.5);
    const assignedNeighborhoods = shuffled.slice(0, 2 + Math.floor(Math.random() * 3));

    // Distribute stops across the selected neighborhoods
    const stops: RouteStop[] = [];
    const now = new Date();

    for (let i = 0; i < totalStops; i++) {
        const neighborhood = assignedNeighborhoods[i % assignedNeighborhoods.length];
        const location = scatterPoint(neighborhood.center, neighborhood.radius);

        // Assign SLA 90 seconds per stop into the future coupled with a base buffer. 
        // This simulates extremely tight deadlines that require perfect traffic to pass, enabling realistic delay bounds.
        const slaDeadline = new Date(now.getTime() + (i * 90 * 1000) + 120000);

        stops.push({
            stop_index: i,
            customer_id: generateCustomerId(vanIndex, i),
            location,
            sla_deadline: slaDeadline.toISOString(),
            parcels: 1 + Math.floor(Math.random() * 5),
        });
    }

    // Pick the van's specific origin Micro-Hub based on its ID modulo
    const originHub = HUBS[vanIndex % HUBS.length];

    // Build dense waypoints via OSRM: HUB → stop[0] → stop[1] → ... → stop[n] → HUB
    const allPoints: Waypoint[] = [originHub, ...stops.map((s) => s.location), originHub];

    // Fetch real geography streets polyline
    const waypoints = await fetchOsrmRoute(allPoints);

    return { route_id: routeId, van_id: vanId, stops, waypoints };
}

/**
 * Generate routes for the entire fleet sequentially to respect OSRM HTTP throttles.
 */
export async function generateFleetRoutes(
    vanCount: number,
    stopsPerVan: number = 18,
    onRouteGenerated?: (route: VanRoute) => void
): Promise<VanRoute[]> {
    const routes: VanRoute[] = [];
    for (let i = 0; i < vanCount; i++) {
        const stops = stopsPerVan - 4 + Math.floor(Math.random() * 9); // 14–22 stops
        const route = await generateRoute(i, stops);
        routes.push(route);

        if (onRouteGenerated) onRouteGenerated(route);
        // Print progress directly to the console so the user knows we didn't freeze
        if (i % 5 === 0) {
            console.log(`   ... fetched mapping geometries for ${i + 1}/${vanCount} vans`);
        }
        // Implement heavily-compliant 1.5s rate-limit stagger so OSRM API doesn't IP-ban us.
        if (i < vanCount - 1) {
            await new Promise(r => setTimeout(r, 1500));
        }
    }
    return routes;
}
