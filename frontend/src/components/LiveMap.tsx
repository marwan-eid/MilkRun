import { MapContainer, TileLayer, useMapEvents, CircleMarker, Popup, Polyline } from 'react-leaflet';
import { useEffect, useRef, useState } from 'react';
import type { CircleMarker as LeafletCircleMarker } from 'leaflet';
import { VanMarker } from './VanMarker';
import type { VanState } from '../types/van';
import 'leaflet/dist/leaflet.css';

const API_BASE = import.meta.env.VITE_API_URL || '';

interface LiveMapProps {
    vans: Map<string, VanState>;
    selectedVanId: string | null;
    onSelectVan: (vanId: string) => void;
}

/** Amsterdam center coordinates */
const AMSTERDAM_CENTER: [number, number] = [52.3676, 4.9041];
const DEFAULT_ZOOM = 13;

/** What the backend decided for an order (POST /api/dispatch, 202). */
interface Assignment {
    order_id: string;
    van_id: string;
    insert_before: number;
    stops_after_insert: number;
    extra_km: number;
    predicted_arrival: string;
    sla_deadline: string;
    on_time: boolean;
    stops_made_late: number;
}

type Order =
    | { lat: number; lng: number; state: 'sending' }
    | { lat: number; lng: number; state: 'assigned'; assignment: Assignment }
    | { lat: number; lng: number; state: 'failed'; message: string };

const time = (iso: string) => new Date(iso).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });

/**
 * Right-click (long-press on touch screens) drops an ad-hoc order; the backend
 * picks the van and the place in its route, and the map shows its choice.
 */
function DispatchLayer({ vans, onAssigned }: { vans: Map<string, VanState>; onAssigned: (vanId: string) => void }) {
    const [order, setOrder] = useState<Order | null>(null);
    const markerRef = useRef<LeafletCircleMarker | null>(null);
    const clearTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

    useEffect(() => () => clearTimeout(clearTimer.current), []);

    // Show the result as soon as it arrives
    useEffect(() => {
        if (order && order.state !== 'sending') {
            markerRef.current?.openPopup();
        }
    }, [order]);

    useMapEvents({
        contextmenu: async (e) => {
            const { lat, lng } = e.latlng;
            clearTimeout(clearTimer.current);
            setOrder({ lat, lng, state: 'sending' });
            let next: Order;
            try {
                const res = await fetch(`${API_BASE}/api/dispatch`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ latitude: lat, longitude: lng }),
                });
                const body = await res.json().catch(() => ({}));
                if (res.status === 202) {
                    next = { lat, lng, state: 'assigned', assignment: body as Assignment };
                    onAssigned((body as Assignment).van_id);
                } else {
                    next = { lat, lng, state: 'failed', message: body.error ?? `Dispatch failed (HTTP ${res.status})` };
                }
            } catch {
                next = { lat, lng, state: 'failed', message: 'Backend unreachable' };
            }
            setOrder(next);
            clearTimer.current = setTimeout(() => setOrder(null), 20_000);
        },
    });

    if (!order) return null;

    const color = order.state === 'failed' ? '#9ca3af' : '#ef4444';
    const van = order.state === 'assigned' ? vans.get(order.assignment.van_id) : undefined;

    return (
        <>
            {van && (
                <Polyline
                    positions={[[van.location.latitude, van.location.longitude], [order.lat, order.lng]]}
                    pathOptions={{ color: '#ef4444', weight: 2, dashArray: '6 6', opacity: 0.8 }}
                />
            )}
            <CircleMarker
                ref={markerRef}
                center={[order.lat, order.lng]}
                radius={8}
                pathOptions={{ color, fillColor: color, fillOpacity: 0.8 }}
            >
                <Popup>
                    {order.state === 'sending' && 'Finding the best van…'}
                    {order.state === 'failed' && order.message}
                    {order.state === 'assigned' && (
                        <div style={{ fontSize: '12px', lineHeight: 1.5 }}>
                            <strong>Assigned to {order.assignment.van_id}</strong>
                            <br />
                            Stop {order.assignment.insert_before + 1} of {order.assignment.stops_after_insert},
                            {' '}+{order.assignment.extra_km.toFixed(2)} km detour
                            <br />
                            ETA {time(order.assignment.predicted_arrival)}, slot until {time(order.assignment.sla_deadline)}
                            {!order.assignment.on_time && <><br /><span style={{ color: '#ef4444' }}>No van can make the slot</span></>}
                            {order.assignment.stops_made_late > 0 && (
                                <><br /><span style={{ color: '#f59e0b' }}>
                                    Delays {order.assignment.stops_made_late} other stop(s) past their slot
                                </span></>
                            )}
                        </div>
                    )}
                </Popup>
            </CircleMarker>
        </>
    );
}

export function LiveMap({ vans, selectedVanId, onSelectVan }: LiveMapProps) {
    return (
        <MapContainer
            center={AMSTERDAM_CENTER}
            zoom={DEFAULT_ZOOM}
            className="live-map"
            zoomControl={true}
        >
            <TileLayer
                attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
                url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
            />

            <DispatchLayer vans={vans} onAssigned={onSelectVan} />

            {vans.size === 0 ? (
                <div className="loading-overlay">
                    <div className="loading-content">
                        <div className="loading-spinner"></div>
                        <div className="loading-text">Initializing simulation fleet...</div>
                    </div>
                </div>
            ) : (
                Array.from(vans.values()).map(van => (
                    <VanMarker
                        key={van.van_id}
                        van={van}
                        isSelected={van.van_id === selectedVanId}
                        onClick={onSelectVan}
                    />
                ))
            )}
        </MapContainer>
    );
}
