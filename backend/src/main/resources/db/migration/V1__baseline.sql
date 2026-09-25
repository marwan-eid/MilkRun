-- ═══════════════════════════════════════════════════════════
-- Baseline schema.
--
-- Written to be idempotent: databases created before Flyway was introduced
-- (from the old infra/postgres/init.sql) already contain these objects, and
-- Flyway baselines them at version 0 and then runs this file as a no-op.
-- ═══════════════════════════════════════════════════════════

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Geofence zones (delay zones, school zones, etc.)
CREATE TABLE IF NOT EXISTS geofence_zones (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name            VARCHAR(255) NOT NULL,
    zone_type       VARCHAR(50) NOT NULL,
    geometry        GEOMETRY(Polygon, 4326) NOT NULL,
    speed_factor    DECIMAL(3,2) DEFAULT 0.50,
    active          BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_geofence_geometry ON geofence_zones USING GIST(geometry);

-- Routes: one row per van per delivery run
CREATE TABLE IF NOT EXISTS completed_routes (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    route_id        VARCHAR(100) UNIQUE NOT NULL,
    van_id          VARCHAR(50) NOT NULL,
    planned_start   TIMESTAMPTZ NOT NULL,
    actual_start    TIMESTAMPTZ,
    planned_end     TIMESTAMPTZ NOT NULL,
    actual_end      TIMESTAMPTZ,
    total_stops     INT NOT NULL,
    completed_stops INT DEFAULT 0,
    failed_stops    INT DEFAULT 0,
    total_distance_km   DECIMAL(8,2),
    total_duration_min  DECIMAL(8,2),
    avg_speed_kmh       DECIMAL(5,2),
    battery_start_pct   SMALLINT,
    battery_end_pct     SMALLINT,
    status          VARCHAR(20) DEFAULT 'IN_PROGRESS',
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_routes_van ON completed_routes(van_id);
CREATE INDEX IF NOT EXISTS idx_routes_status ON completed_routes(status);

-- Individual delivery logs
CREATE TABLE IF NOT EXISTS delivery_logs (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    route_id            VARCHAR(100) NOT NULL REFERENCES completed_routes(route_id),
    van_id              VARCHAR(50) NOT NULL,
    stop_index          INT NOT NULL,
    customer_id         VARCHAR(100) NOT NULL,
    location            GEOMETRY(Point, 4326),
    sla_deadline        TIMESTAMPTZ NOT NULL,
    actual_arrival      TIMESTAMPTZ,
    actual_departure    TIMESTAMPTZ,
    parcels_delivered   INT DEFAULT 0,
    delivery_status     VARCHAR(20) NOT NULL,
    sla_breached        BOOLEAN DEFAULT FALSE,
    breach_seconds      INT DEFAULT 0,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_delivery_route ON delivery_logs(route_id);
CREATE INDEX IF NOT EXISTS idx_delivery_sla ON delivery_logs(sla_breached) WHERE sla_breached = TRUE;

-- SLA breach events (denormalized for fast analytics)
CREATE TABLE IF NOT EXISTS sla_breaches (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    route_id        VARCHAR(100) NOT NULL,
    van_id          VARCHAR(50) NOT NULL,
    stop_index      INT NOT NULL,
    customer_id     VARCHAR(100) NOT NULL,
    sla_deadline    TIMESTAMPTZ NOT NULL,
    predicted_arrival TIMESTAMPTZ NOT NULL,
    actual_arrival  TIMESTAMPTZ,
    breach_seconds  INT NOT NULL,
    severity        VARCHAR(10) NOT NULL,
    cause           VARCHAR(50),
    geofence_id     UUID REFERENCES geofence_zones(id),
    detected_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_breach_van ON sla_breaches(van_id);
CREATE INDEX IF NOT EXISTS idx_breach_severity ON sla_breaches(severity);
CREATE INDEX IF NOT EXISTS idx_breach_detected ON sla_breaches(detected_at);

-- GPS event archive (sampled, for path replay)
CREATE TABLE IF NOT EXISTS gps_archive (
    id              BIGSERIAL PRIMARY KEY,
    event_id        UUID NOT NULL,
    van_id          VARCHAR(50) NOT NULL,
    route_id        VARCHAR(100) NOT NULL,
    location        GEOMETRY(Point, 4326) NOT NULL,
    speed_kmh       DECIMAL(5,2),
    heading         DECIMAL(5,2),
    battery_pct     SMALLINT,
    device_timestamp    TIMESTAMPTZ NOT NULL,
    ingestion_timestamp TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gps_van_time ON gps_archive(van_id, device_timestamp);
CREATE INDEX IF NOT EXISTS idx_gps_route ON gps_archive(route_id);
CREATE INDEX IF NOT EXISTS idx_gps_location ON gps_archive USING GIST(location);

-- Dead letter log (for audit/debugging)
CREATE TABLE IF NOT EXISTS dead_letter_log (
    id              BIGSERIAL PRIMARY KEY,
    original_topic  VARCHAR(100) NOT NULL,
    van_id          VARCHAR(50),
    event_payload   JSONB NOT NULL,
    error_reason    VARCHAR(255) NOT NULL,
    reconciled      BOOLEAN DEFAULT FALSE,
    reconciled_at   TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dlq_reconciled ON dead_letter_log(reconciled) WHERE reconciled = FALSE;

-- Seed geofence zones in Amsterdam (skipped when a zone with the same name exists)
INSERT INTO geofence_zones (name, zone_type, geometry, speed_factor)
SELECT v.name, v.zone_type, ST_GeomFromText(v.wkt, 4326), v.speed_factor
FROM (VALUES
    ('Vondelpark Construction', 'CONSTRUCTION',
     'POLYGON((4.8650 52.3580, 4.8750 52.3580, 4.8750 52.3620, 4.8650 52.3620, 4.8650 52.3580))', 0.40),
    ('Central Station Traffic', 'TRAFFIC',
     'POLYGON((4.8950 52.3770, 4.9050 52.3770, 4.9050 52.3810, 4.8950 52.3810, 4.8950 52.3770))', 0.50),
    ('De Pijp School Zone', 'SCHOOL',
     'POLYGON((4.8900 52.3500, 4.8980 52.3500, 4.8980 52.3540, 4.8900 52.3540, 4.8900 52.3500))', 0.60),
    ('Jordaan Narrow Streets', 'TRAFFIC',
     'POLYGON((4.8780 52.3700, 4.8870 52.3700, 4.8870 52.3740, 4.8780 52.3740, 4.8780 52.3700))', 0.55)
) AS v(name, zone_type, wkt, speed_factor)
WHERE NOT EXISTS (SELECT 1 FROM geofence_zones g WHERE g.name = v.name);
