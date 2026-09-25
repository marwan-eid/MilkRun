-- Each row written from a Kafka record carries that record's event id, so a
-- replayed record (after a crash or rebalance) is ignored instead of written
-- twice. Rows written before this migration keep a NULL event id.

ALTER TABLE delivery_logs ADD COLUMN IF NOT EXISTS event_id UUID;
CREATE UNIQUE INDEX IF NOT EXISTS uq_delivery_logs_event ON delivery_logs(event_id);

ALTER TABLE sla_breaches ADD COLUMN IF NOT EXISTS event_id UUID;
CREATE UNIQUE INDEX IF NOT EXISTS uq_sla_breaches_event ON sla_breaches(event_id);

-- gps_archive was never written before this version, so it holds no duplicates.
CREATE UNIQUE INDEX IF NOT EXISTS uq_gps_archive_event ON gps_archive(event_id);

-- Route counters are derived from the delivery log.
CREATE INDEX IF NOT EXISTS idx_delivery_route_status ON delivery_logs(route_id, delivery_status);
