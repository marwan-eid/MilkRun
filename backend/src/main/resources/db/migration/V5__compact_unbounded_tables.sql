-- Until this version nothing was ever deleted. On the demo VM gps_archive
-- reached 21 GB with its indexes and dead_letter_log 4 GB, filling 92% of the
-- disk, and the analytics queries over sla_breaches outgrew their timeout.
-- From now on RetentionJob deletes old rows, but deleted rows only free space
-- inside the table files, so the files would stay this size. This rebuilds the
-- three tables with only their recent rows, which gives the space back at once.
--
-- TRUNCATE plus re-insert keeps each table's definition, indexes, constraints
-- and id sequence. It is transactional: if anything fails, nothing changes.
--
-- The newest rows are found by walking the primary key backwards with a LIMIT
-- rather than by a filter on created_at, which has no index and would read
-- every page of an 11 GB table. Each LIMIT is well above the rows the window
-- holds at the demo's rate (about 1.5 million GPS points and 100 000 dead
-- letters a day).

-- GPS points: the last 3 hours. Routes take under an hour, and a route's
-- points are only read when it completes (for its distance).
CREATE TEMP TABLE keep_gps ON COMMIT DROP AS
SELECT * FROM (SELECT * FROM gps_archive ORDER BY id DESC LIMIT 400000) recent
WHERE created_at > now() - INTERVAL '3 hours';

-- Dead letters: the last 2 days, as RetentionJob keeps.
CREATE TEMP TABLE keep_dlq ON COMMIT DROP AS
SELECT * FROM (SELECT * FROM dead_letter_log ORDER BY id DESC LIMIT 400000) recent
WHERE created_at > now() - INTERVAL '2 days';

-- SLA breaches: the last 7 days (detected_at is indexed).
CREATE TEMP TABLE keep_breaches ON COMMIT DROP AS
SELECT * FROM sla_breaches WHERE detected_at > now() - INTERVAL '7 days';

TRUNCATE gps_archive, dead_letter_log, sla_breaches;

INSERT INTO gps_archive SELECT * FROM keep_gps;
INSERT INTO dead_letter_log SELECT * FROM keep_dlq;
INSERT INTO sla_breaches SELECT * FROM keep_breaches;

ANALYZE gps_archive;
ANALYZE dead_letter_log;
ANALYZE sla_breaches;
