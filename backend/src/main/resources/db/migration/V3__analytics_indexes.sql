-- Analytics join breaches to zones and vans; without these the joins scan
-- the whole (append-only, ever-growing) breach table.
CREATE INDEX IF NOT EXISTS idx_breach_geofence ON sla_breaches(geofence_id);
CREATE INDEX IF NOT EXISTS idx_routes_van_status ON completed_routes(van_id, status);
