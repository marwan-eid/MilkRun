import { describe, it, expect } from 'vitest';
import { mergeUpdates, pruneSilent } from './fleet';
import type { VanState } from '../types/van';

function van(id: string, lastUpdated: string, lat = 52.37): VanState {
    return {
        van_id: id, route_id: 'r', location: { latitude: lat, longitude: 4.9 }, speed_kmh: 25, heading_degrees: 0,
        battery_pct: 80, status: 'EN_ROUTE', current_stop_index: 0, total_stops: 10, eta_next_stop_seconds: 60,
        sla_risk: 'NONE', confidence: 'REAL_TIME', in_geofence: false, geofence_name: null, last_updated: lastUpdated,
        sla_deadline: null, predicted_arrival: null, sla_slack_seconds: null, eta_method: 'ROUTE',
    };
}

describe('mergeUpdates', () => {
    it('adds new vans and replaces older states', () => {
        const prev = new Map([['a', van('a', '2026-09-25T10:00:00Z', 52.30)]]);
        const next = mergeUpdates(prev, [van('a', '2026-09-25T10:00:01Z', 52.31), van('b', '2026-09-25T10:00:00Z')]);
        expect(next.size).toBe(2);
        expect(next.get('a')!.location.latitude).toBe(52.31);
        expect(prev.get('a')!.location.latitude).toBe(52.30);
    });

    it('ignores a state older than the one held (e.g. from a snapshot)', () => {
        const prev = new Map([['a', van('a', '2026-09-25T10:00:05Z', 52.35)]]);
        const next = mergeUpdates(prev, [van('a', '2026-09-25T10:00:01Z', 52.31)]);
        expect(next).toBe(prev);
    });

    it('returns the same map when there is nothing to apply', () => {
        const prev = new Map([['a', van('a', '2026-09-25T10:00:00Z')]]);
        expect(mergeUpdates(prev, [])).toBe(prev);
    });
});

describe('pruneSilent', () => {
    it('drops vans not heard from within the limit', () => {
        const vans = new Map([['a', van('a', 'x')], ['b', van('b', 'x')]]);
        const receivedAt = new Map([['a', 1_000], ['b', 100_000]]);
        const next = pruneSilent(vans, receivedAt, 125_000, 120_000);
        expect([...next.keys()]).toEqual(['b']);
        expect(receivedAt.has('a')).toBe(false);
    });

    it('returns the same map when every van is fresh', () => {
        const vans = new Map([['a', van('a', 'x')]]);
        expect(pruneSilent(vans, new Map([['a', 10_000]]), 20_000, 120_000)).toBe(vans);
    });
});
