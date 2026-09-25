-- Route plans arrive from the simulator (route-plans topic) and fill in the
-- planned side of each route; ETA predictions are recorded with breaches.

ALTER TABLE completed_routes ADD COLUMN IF NOT EXISTS plan_version INT;
ALTER TABLE completed_routes ADD COLUMN IF NOT EXISTS time_scale NUMERIC(6,2);
ALTER TABLE completed_routes ADD COLUMN IF NOT EXISTS planned_distance_km NUMERIC(8,2);

-- The prediction may be unknown (e.g. no route plan received yet); it used to
-- be filled with the actual arrival, which made it meaningless.
ALTER TABLE sla_breaches ALTER COLUMN predicted_arrival DROP NOT NULL;
