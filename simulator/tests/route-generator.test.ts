import { describe, it, expect } from 'vitest';
import { generateRoute, generateFleetRoutes, haversineDistance, calculateBearing } from '../src/route-generator.js';

describe('Route Generator', () => {
    describe('generateRoute', () => {
        it('should generate a route with the correct number of stops', () => {
            const route = generateRoute(0, 15);
            expect(route.stops).toHaveLength(15);
            expect(route.van_id).toBe('van-000');
            expect(route.route_id).toContain('van-000');
        });

        it('should assign unique customer IDs to each stop', () => {
            const route = generateRoute(1, 10);
            const customerIds = route.stops.map((s) => s.customer_id);
            const unique = new Set(customerIds);
            expect(unique.size).toBe(customerIds.length);
        });

        it('should generate stops with valid coordinates in Amsterdam area', () => {
            const route = generateRoute(0, 20);
            for (const stop of route.stops) {
                // Amsterdam bounding box (roughly)
                expect(stop.location.latitude).toBeGreaterThan(52.28);
                expect(stop.location.latitude).toBeLessThan(52.42);
                expect(stop.location.longitude).toBeGreaterThan(4.82);
                expect(stop.location.longitude).toBeLessThan(4.98);
            }
        });

        it('should generate dense waypoints between stops', () => {
            const route = generateRoute(0, 5);
            // Waypoints should be denser than just stops + hub endpoints
            expect(route.waypoints.length).toBeGreaterThan(5);
        });

        it('should start and end waypoints at the hub location', () => {
            const route = generateRoute(0, 5);
            const first = route.waypoints[0];
            const last = route.waypoints[route.waypoints.length - 1];

            // Hub is at Science Park: 52.3548, 4.9578
            expect(first.latitude).toBeCloseTo(52.3548, 3);
            expect(first.longitude).toBeCloseTo(4.9578, 3);
            expect(last.latitude).toBeCloseTo(52.3548, 3);
            expect(last.longitude).toBeCloseTo(4.9578, 3);
        });

        it('should have SLA deadlines in chronological order', () => {
            const route = generateRoute(0, 10);
            for (let i = 1; i < route.stops.length; i++) {
                const prev = new Date(route.stops[i - 1].sla_deadline).getTime();
                const curr = new Date(route.stops[i].sla_deadline).getTime();
                expect(curr).toBeGreaterThan(prev);
            }
        });
    });

    describe('generateFleetRoutes', () => {
        it('should generate the correct number of van routes', () => {
            const routes = generateFleetRoutes(10);
            expect(routes).toHaveLength(10);
        });

        it('should assign unique van IDs', () => {
            const routes = generateFleetRoutes(5);
            const vanIds = routes.map((r) => r.van_id);
            expect(new Set(vanIds).size).toBe(5);
        });

        it('should generate routes with varying stop counts', () => {
            const routes = generateFleetRoutes(20);
            const stopCounts = routes.map((r) => r.stops.length);
            const unique = new Set(stopCounts);
            // With 20 routes and ±4 stops, we should see some variation
            expect(unique.size).toBeGreaterThan(1);
        });
    });

    describe('haversineDistance', () => {
        it('should calculate approximately correct distances', () => {
            // Amsterdam Centraal to Dam Square ≈ 0.7 km
            const dist = haversineDistance(
                { latitude: 52.3791, longitude: 4.9003 },
                { latitude: 52.3730, longitude: 4.8932 },
            );
            expect(dist).toBeGreaterThan(0.5);
            expect(dist).toBeLessThan(1.5);
        });

        it('should return 0 for the same point', () => {
            const point = { latitude: 52.37, longitude: 4.90 };
            expect(haversineDistance(point, point)).toBe(0);
        });
    });

    describe('calculateBearing', () => {
        it('should return ~0 for due north', () => {
            const bearing = calculateBearing(
                { latitude: 52.37, longitude: 4.90 },
                { latitude: 52.38, longitude: 4.90 },
            );
            expect(bearing).toBeCloseTo(0, 0);
        });

        it('should return ~90 for due east', () => {
            const bearing = calculateBearing(
                { latitude: 52.37, longitude: 4.90 },
                { latitude: 52.37, longitude: 4.92 },
            );
            expect(bearing).toBeCloseTo(90, 0);
        });

        it('should return values between 0 and 360', () => {
            for (let i = 0; i < 20; i++) {
                const bearing = calculateBearing(
                    { latitude: 52.37 + Math.random() * 0.1, longitude: 4.90 + Math.random() * 0.1 },
                    { latitude: 52.37 + Math.random() * 0.1, longitude: 4.90 + Math.random() * 0.1 },
                );
                expect(bearing).toBeGreaterThanOrEqual(0);
                expect(bearing).toBeLessThan(360);
            }
        });
    });
});
