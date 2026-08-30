import { MapContainer, TileLayer } from 'react-leaflet';
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
