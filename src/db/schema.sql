-- era5_land_base_grid DDL (PLANNING.md §8.1).
-- Global, static, deterministic grid: one row per 0.25° cell. Shipped as a seed.

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS era5_land_base_grid (
    child_id   CHAR(4)          NOT NULL,
    parent_id  CHAR(4)          NOT NULL,
    lat        DOUBLE PRECISION NOT NULL,            -- cell center, -90..90
    lon        DOUBLE PRECISION NOT NULL,            -- cell center, -180..180
    is_land    BOOLEAN          NOT NULL,
    elevation  REAL,                                 -- meters (z / 9.80665)
    geom       GEOMETRY(Polygon, 4326) NOT NULL,     -- 0.25° square cell
    PRIMARY KEY (child_id)
);

CREATE INDEX IF NOT EXISTS idx_grid_parent ON era5_land_base_grid (parent_id);
CREATE INDEX IF NOT EXISTS idx_grid_geom   ON era5_land_base_grid USING GIST (geom);
