-- The analytics history tables still held everything since September:
-- sla_breaches 860k rows (158 MB, scanned whole by three of the dashboard's
-- queries), completed_routes 104k, delivery_logs 3.6M (1.1 GB). Trimming that
-- backlog row by row kept the demo VM's slow disk saturated for hours, and
-- the table files would not have shrunk anyway, so the dashboard's scans
-- would have stayed as slow. This rebuilds the tables instead.
--
--  * sla_breaches and completed_routes keep their last 7 days (what
--    RetentionJob keeps from now on), read with one sequential scan each:
--    on the VM that takes seconds, while the same rows fetched through an
--    index took minutes.
--  * delivery_logs is emptied. Nothing reads a route's deliveries after the
--    route completes (its counters are then stored in completed_routes), and
--    a deploy restarts the simulator, which abandons the routes under way.
--
-- TRUNCATE plus re-insert keeps each table's definition, indexes and
-- constraints, and runs in this migration's transaction: if anything fails,
-- nothing changes.

SET LOCAL enable_indexscan = off;
SET LOCAL enable_indexonlyscan = off;
SET LOCAL enable_bitmapscan = off;

CREATE TEMP TABLE keep_breaches ON COMMIT DROP AS
SELECT * FROM sla_breaches WHERE detected_at > now() - INTERVAL '7 days';

CREATE TEMP TABLE keep_routes ON COMMIT DROP AS
SELECT * FROM completed_routes WHERE created_at > now() - INTERVAL '7 days';

TRUNCATE delivery_logs, completed_routes, sla_breaches;

INSERT INTO completed_routes SELECT * FROM keep_routes;
INSERT INTO sla_breaches SELECT * FROM keep_breaches;

-- RetentionJob picks the oldest routes by creation time.
CREATE INDEX IF NOT EXISTS idx_routes_created ON completed_routes(created_at);

ANALYZE completed_routes;
ANALYZE sla_breaches;
ANALYZE delivery_logs;
