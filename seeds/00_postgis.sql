-- Runs first (lexical order) on a fresh Postgres data dir so PostGIS exists before
-- the grid seed restores. Mounted at /docker-entrypoint-initdb.d (PLANNING.md §8.1).
CREATE EXTENSION IF NOT EXISTS postgis;
