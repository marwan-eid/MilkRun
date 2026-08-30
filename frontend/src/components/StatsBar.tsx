interface StatsBarProps {
    connected: boolean;
    vanCount: number;
    lastEvent: number;
}

export function StatsBar({ connected, vanCount, lastEvent }: StatsBarProps) {
    const lag = lastEvent > 0 ? Math.floor((Date.now() - lastEvent) / 1000) : -1;

    return (
        <div className="stats-bar">
            <div className="stats-left">
                <span className="app-title">🥛 The Milk-Run</span>
                <span className="app-subtitle">Live Delivery Tracker</span>
            </div>
            <div className="stats-right">
                <span className={`connection-badge ${connected ? 'connected' : 'disconnected'}`}>
                    {connected ? '● Connected' : '○ Disconnected'}
                </span>
                <span className="stat-item">
                    🚐 {vanCount} vans
                </span>
                {lag >= 0 && (
                    <span className={`stat-item ${lag > 5 ? 'stale' : ''}`}>
                        ⏱ {lag}s ago
                    </span>
                )}
            </div>
        </div>
    );
}
