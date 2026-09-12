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
  const [panelOpen, setPanelOpen] = useState(false);

  const criticalCount = Array.from(vans.values()).filter(v => v.sla_risk === 'CRITICAL').length;

  return (
    <div className="app">
      <StatsBar
        connected={connected}
        vanCount={vans.size}
        lastEvent={lastEvent}
      />
      <div className="main-content" style={{ position: 'relative' }}>

        {/* Dynamic Dispatch UI Hint Overlay */}
        <div className="dispatch-hint">
          💡 <span className="hint-desktop" style={{ color: 'var(--text-primary)' }}>Right-Click</span>
          <span className="hint-mobile" style={{ color: 'var(--text-primary)' }}>Long-Press</span>
          {' '}anywhere to dispatch an order
        </div>

        <LiveMap
          vans={vans}
          selectedVanId={selectedVanId}
          onSelectVan={setSelectedVanId}
        />

        {/* Mobile backdrop */}
        <div
          className={`panel-backdrop ${panelOpen ? 'active' : ''}`}
          onClick={() => setPanelOpen(false)}
        />

        <SlaPanel
          vans={vans}
          selectedVanId={selectedVanId}
          onSelectVan={setSelectedVanId}
          isOpen={panelOpen}
          onClose={() => setPanelOpen(false)}
        />
      </div>

      {/* Mobile fleet toggle */}
      <button
        className="mobile-panel-toggle"
        onClick={() => setPanelOpen(true)}
      >
        🚐 Fleet
        {criticalCount > 0 && <span className="badge">{criticalCount}</span>}
      </button>

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
