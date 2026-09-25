-- Runs once when the database volume is created.
--
-- The schema itself is managed by Flyway migrations in
-- backend/src/main/resources/db/migration and applied when the backend starts.
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
