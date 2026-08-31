import { MapContainer, TileLayer, useMapEvents, CircleMarker, Popup } from 'react-leaflet';
import { useState } from 'react';
import { VanMarker } from './VanMarker';
import type { VanState } from '../types/van';
import 'leaflet/dist/leaflet.css';

interface LiveMapProps {
    vans: Map<string, VanState>;
    selectedVanId: string | null;
    onSelectVan: (vanId: string) => void;
}

/** Amsterdam center coordinates */
const AMSTERDAM_CENTER: [number, number] = [52.3676, 4.9041];
const DEFAULT_ZOOM = 13;

export function LiveMap({ vans, selectedVanId, onSelectVan }: LiveMapProps) {

    // Inner component strictly to securely bind into the Leaflet DOM context
    function DispatchLayer() {
        const [clickPos, setClickPos] = useState<{ lat: number, lng: number } | null>(null);

        useMapEvents({
            contextmenu: async (e) => {
                const { lat, lng } = e.latlng;
                setClickPos({ lat, lng });

                try {
                    await fetch('/api/dispatch', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ latitude: lat, longitude: lng })
                    });

                    // Dismiss the temporary UI map ping after 5 seconds visually
                    setTimeout(() => setClickPos(null), 5000);
                } catch (err) {
                    console.error('Failed to trigger native dynamic dispatch to Backend Engine:', err);
                }
            }
        });

        return clickPos ? (
            <CircleMarker
                center={[clickPos.lat, clickPos.lng]}
                radius={8}
                pathOptions={{ color: '#ef4444', fillColor: '#ef4444', fillOpacity: 0.8 }}
            >
                <Popup>Ad-Hoc Order Dispatched to Nearest Van!</Popup>
            </CircleMarker>
        ) : null;
    }

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

            <DispatchLayer />

            {Array.from(vans.values()).map(van => (
                <VanMarker
                    key={van.van_id}
                    van={van}
                    isSelected={van.van_id === selectedVanId}
                    onClick={onSelectVan}
                />
            ))}
        </MapContainer>
    );
}
