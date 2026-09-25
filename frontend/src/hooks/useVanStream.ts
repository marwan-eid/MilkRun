import { useState, useEffect, useRef } from 'react';
import type { VanState } from '../types/van';
import { mergeUpdates, pruneSilent } from '../lib/fleet';

const API_BASE = import.meta.env.VITE_API_URL || '';
/** Updates are applied to React state at most this often (~15 fps). */
const FLUSH_MS = 66;
/** A van that has not reported for this long is removed from the map. */
const SILENT_MS = 120_000;

/**
 * Subscribes to the backend's SSE stream and keeps a map of every van's latest state.
 *
 * - Loads a snapshot from /api/vans on every (re)connect, so the map is full
 *   immediately instead of filling as vans report.
 * - Batches updates and applies them at ~15 fps to keep rendering cheap.
 * - Reconnects with exponential backoff (1 s up to 30 s).
 * - Removes vans that have gone silent (e.g. after the simulator restarts).
 */
export function useVanStream() {
    const [vans, setVans] = useState<Map<string, VanState>>(new Map());
    const [connected, setConnected] = useState(false);
    const [lastEvent, setLastEvent] = useState<number>(0);
    const pending = useRef<Map<string, VanState>>(new Map());
    const receivedAt = useRef<Map<string, number>>(new Map());

    useEffect(() => {
        let source: EventSource | null = null;
        let reconnectTimer: ReturnType<typeof setTimeout> | undefined;
        let reconnectDelay = 1000;
        let closed = false;

        const enqueue = (van: VanState) => {
            pending.current.set(van.van_id, van);
            receivedAt.current.set(van.van_id, Date.now());
        };

        const loadSnapshot = async () => {
            try {
                const res = await fetch(`${API_BASE}/api/vans`);
                if (!res.ok) return;
                const snapshot: VanState[] = await res.json();
                snapshot.forEach((van) => {
                    if (!pending.current.has(van.van_id)) enqueue(van);
                });
            } catch {
                // The stream fills the map anyway
            }
        };

        function connect() {
            source?.close();
            const es = new EventSource(`${API_BASE}/api/stream/vans`);
            source = es;

            es.addEventListener('van-update', (event: MessageEvent) => {
                try {
                    enqueue(JSON.parse(event.data));
                } catch (err) {
                    console.warn('Failed to parse van-update:', err);
                }
            });

            es.onopen = () => {
                setConnected(true);
                reconnectDelay = 1000;
                void loadSnapshot();
            };

            es.onerror = () => {
                setConnected(false);
                es.close();
                if (closed) return;
                const delay = reconnectDelay;
                reconnectDelay = Math.min(delay * 2, 30_000);
                reconnectTimer = setTimeout(connect, delay);
            };
        }

        connect();
        const flush = setInterval(() => {
            const updates = Array.from(pending.current.values());
            pending.current.clear();
            const now = Date.now();
            setVans((prev) => pruneSilent(mergeUpdates(prev, updates), receivedAt.current, now, SILENT_MS));
            if (updates.length > 0) setLastEvent(now);
        }, FLUSH_MS);

        return () => {
            closed = true;
            source?.close();
            clearTimeout(reconnectTimer);
            clearInterval(flush);
        };
    }, []);

    return { vans, connected, lastEvent };
}
