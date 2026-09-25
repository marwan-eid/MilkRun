import { describe, it, expect } from 'vitest';
import { applyAssignment, type DispatchAssignment } from '../src/dispatch-consumer.js';
import { VanSimulator, type EventSink } from '../src/van-simulator.js';
import { straightLinePath, StraightLineRouter } from '../src/route-generator.js';
import { type VanRoute, type Waypoint } from '../src/models/index.js';

const sink: EventSink = {
    async sendGpsEvents() { },
    async sendDeliveryEvent() { },
    async sendRoutePlan() { },
};

function van(id: string, routeId: string): VanSimulator {
    const points: Waypoint[] = [
        { latitude: 52.300, longitude: 4.85 },
        { latitude: 52.305, longitude: 4.85 },
        { latitude: 52.310, longitude: 4.85 },
    ];
    const path = straightLinePath(points);
    const far = new Date(Date.UTC(2030, 0, 1)).toISOString();
    const route: VanRoute = {
        route_id: routeId, van_id: id, created_at: new Date(0).toISOString(), waypoints: path.waypoints,
        stops: [{
            stop_index: 0, customer_id: 'c0', location: points[1], sla_deadline: far, planned_arrival: far,
            parcels: 1, waypoint_index: path.legEndIndices[0],
        }],
    };
    return new VanSimulator(route, sink, { chaosEnabled: false, router: new StraightLineRouter(), now: () => 0 });
}

const assignment = (over: Partial<DispatchAssignment> = {}): DispatchAssignment => ({
    order_id: '0123456789abcdef', van_id: 'van-1', route_id: 'r-1', latitude: 52.302, longitude: 4.853,
    insert_before: 0, sla_deadline: '2030-01-01T00:00:00Z', ...over,
});

describe('applyAssignment', () => {
    it('inserts the order into the assigned van at the requested position', async () => {
        const vans = [van('van-1', 'r-1'), van('van-2', 'r-2')];
        await vans[0].begin();
        const result = await applyAssignment(vans, assignment());
        expect(result).toEqual({ status: 'inserted', vanId: 'van-1', index: 0 });
        expect(vans[0].stops.map((s) => s.customer_id)).toEqual(['order-01234567', 'c0']);
        expect(vans[1].stops).toHaveLength(1);
    });

    it('rejects when the van has moved on to another route', async () => {
        const vans = [van('van-1', 'r-2')];
        await vans[0].begin();
        expect(await applyAssignment(vans, assignment())).toMatchObject({ status: 'rejected' });
    });

    it('rejects unknown vans and malformed messages', async () => {
        const vans = [van('van-1', 'r-1')];
        await vans[0].begin();
        expect(await applyAssignment(vans, assignment({ van_id: 'van-9' }))).toMatchObject({ status: 'rejected' });
        // An old-format message (before assignments carried a van)
        expect(await applyAssignment(vans, { eventId: 'x', latitude: 52.3, longitude: 4.8 } as unknown as DispatchAssignment))
            .toMatchObject({ status: 'rejected', reason: 'malformed assignment' });
    });
});
