-- Until this version nothing was ever deleted. On the demo VM gps_archive
-- reached 21 GB with its indexes and dead_letter_log 4 GB, filling 92% of the
-- disk. From now on RetentionJob deletes old rows, but deleted rows only free
-- space inside the table files, so the files would stay this size; TRUNCATE
-- gives the space back at once and keeps each table's definition, indexes and
-- id sequence.
--
-- Nothing is copied over: the VM's disk reads about 1 MB/s at random, so even
-- the newest few hundred thousand rows took minutes to read, longer than a
-- deploy waits for the backend (a first version of this migration did that
-- and was rolled back). What is lost is small:
--  * gps_archive: a route's points are only read when that route completes,
--    and a deploy restarts the simulator, which abandons the routes under way;
--  * dead_letter_log: an audit log, shown only as counts by reason.
--
-- sla_breaches, delivery_logs and completed_routes are what the analytics
-- report on; they are smaller and RetentionJob trims them to 7 days.

TRUNCATE gps_archive, dead_letter_log;
