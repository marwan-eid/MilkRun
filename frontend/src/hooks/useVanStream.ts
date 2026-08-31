import { useState, useEffect, useRef, useCallback } from 'react';
import type { VanState } from '../types/van';

const API_BASE = import.meta.env.VITE_API_URL || '';

/**
 * React hook that consumes the backend's SSE stream and maintains
 * a real-time map of all van states.
 *
 * Features:
 * - Auto-reconnect on connection loss (with exponential backoff)
 * - Tracks connection status for UI indicators
 * - Aggregates van states into a Map keyed by van_id
 */
export function useVanStream() {
    const [vans, setVans] = useState<Map<string, VanState>>(new Map());
    const [connected, setConnected] = useState(false);
    const [lastEvent, setLastEvent] = useState<number>(0);
    const eventSourceRef = useRef<EventSource | null>(null);
    const reconnectTimeoutRef = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
    const reconnectDelayRef = useRef(1000);

    const connect = useCallback(() => {
        // Close existing connection
        if (eventSourceRef.current) {
            eventSourceRef.current.close();
        }

        const es = new EventSource(`${API_BASE}/api/stream/vans`);
        eventSourceRef.current = es;

        es.addEventListener('van-update', (event: MessageEvent) => {
            try {
                const vanState: VanState = JSON.parse(event.data);
                setVans(prev => {
                    const next = new Map(prev);
                    next.set(vanState.van_id, vanState);
                    return next;
                });
                setLastEvent(Date.now());
            } catch (err) {
                console.warn('Failed to parse van-update:', err);
            }
        });

        es.onopen = () => {
            setConnected(true);
            reconnectDelayRef.current = 1000; // Reset backoff
            console.log('🔗 SSE connected');
        };

        es.onerror = () => {
            setConnected(false);
            es.close();

            // Exponential backoff reconnect
            const delay = reconnectDelayRef.current;
            console.log(`🔌 SSE disconnected, reconnecting in ${delay}ms...`);

            reconnectTimeoutRef.current = setTimeout(() => {
                reconnectDelayRef.current = Math.min(delay * 2, 30000);
                connect();
            }, delay);
        };
    }, []);

    useEffect(() => {
        connect();
        return () => {
            eventSourceRef.current?.close();
            if (reconnectTimeoutRef.current) {
                clearTimeout(reconnectTimeoutRef.current);
            }
        };
    }, [connect]);

    return { vans, connected, lastEvent };
}
