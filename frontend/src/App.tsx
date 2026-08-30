import { useState } from 'react';
import { useVanStream } from './hooks/useVanStream';
import { LiveMap } from './components/LiveMap';
import { SlaPanel } from './components/SlaPanel';
import { StatsBar } from './components/StatsBar';
import { AnalyticsPanel } from './components/AnalyticsPanel';
import './App.css';

export default function App() {
  const { vans, connected, lastEvent } = useVanStream();
  const [selectedVanId, setSelectedVanId] = useState<string | null>(null);
  const [showAnalytics, setShowAnalytics] = useState(false);

  return (
    <div className="app">
      <StatsBar
        connected={connected}
        vanCount={vans.size}
        lastEvent={lastEvent}
      />
      <div className="main-content">
        <LiveMap
          vans={vans}
          selectedVanId={selectedVanId}
          onSelectVan={setSelectedVanId}
        />
        <SlaPanel
          vans={vans}
          selectedVanId={selectedVanId}
          onSelectVan={setSelectedVanId}
        />
      </div>

      {/* Analytics toggle button */}
      <button
        className="analytics-fab"
        onClick={() => setShowAnalytics(true)}
        title="Open Fleet Analytics (Apache Calcite)"
      >
        📊
      </button>

      <AnalyticsPanel
        visible={showAnalytics}
        onClose={() => setShowAnalytics(false)}
      />
    </div>
  );
}
