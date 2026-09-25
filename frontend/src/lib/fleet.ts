import type { VanState } from '../types/van';

/**
 * Applies updates to the fleet map, keeping whichever state of a van is newer
 * (a snapshot fetched on reconnect can be older than a streamed update).
 * Returns the same map when nothing changed.
 */
export function mergeUpdates(prev: Map<string, VanState>, updates: Iterable<VanState>): Map<string, VanState> {
    let next: Map<string, VanState> | null = null;
    for (const van of updates) {
        const current = (next ?? prev).get(van.van_id);
        if (current && Date.parse(current.last_updated) > Date.parse(van.last_updated)) continue;
        next ??= new Map(prev);
        next.set(van.van_id, van);
    }
    return next ?? prev;
}

/**
 * Drops vans not heard from for maxAgeMs, measured with the browser's own
 * clock (when each update was received), so a skewed server clock cannot
 * empty the map. Returns the same map when nothing was dropped.
 */
export function pruneSilent(vans: Map<string, VanState>, receivedAt: Map<string, number>, now: number,
    maxAgeMs: number): Map<string, VanState> {
    let next: Map<string, VanState> | null = null;
    for (const id of vans.keys()) {
        const seen = receivedAt.get(id);
        if (seen === undefined || now - seen > maxAgeMs) {
            next ??= new Map(vans);
            next.delete(id);
            receivedAt.delete(id);
        }
    }
    return next ?? vans;
}
