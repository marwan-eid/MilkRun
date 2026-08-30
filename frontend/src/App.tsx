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
      <div className="main-content" style={{ position: 'relative' }}>

        {/* Dynamic Dispatch UI Hint Overlay */}
        <div style={{
          position: 'absolute',
          top: '16px',
          left: 'calc(50% - 160px)', /* Centered over the map portion (accounting for the 320px SLA Panel on the right) */
          transform: 'translateX(-50%)',
          background: 'var(--glass)',
          border: '1px solid var(--glass-border)',
          padding: '8px 20px',
          borderRadius: '24px',
          zIndex: 400,
          color: 'var(--text-secondary)',
          fontSize: '13px',
          fontWeight: 500,
          pointerEvents: 'none',
          backdropFilter: 'blur(12px)',
          boxShadow: 'var(--shadow)',
          display: 'flex',
          alignItems: 'center',
          gap: '8px',
          animation: 'slideUp 0.5s ease-out'
        }}>
          💡 <span style={{ color: 'var(--text-primary)' }}>Right-Click</span> anywhere to dispatch an order
        </div>

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
