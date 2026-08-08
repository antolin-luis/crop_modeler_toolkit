-- chirps_base_grid DDL — the 0.05° fine grid (src/grid/fine_spec.py).
--
-- Kept separate from schema.sql for the same reason silver_schema.sql is: schema.sql ships
-- with the seed and runs once at first boot via /docker-entrypoint-initdb.d. This table is
-- created by the chirps_grid_build DAG instead, so an existing database picks it up with no
-- volume reset.
--
-- **Region-scoped, unlike era5_land_base_grid.** A global 0.05° grid is 17,280,000 rows —
-- too big to ship. Only cells inside a requested extent are materialized, and extents
-- accumulate: building a second region adds rows rather than replacing them. The *codes*
-- stay globally deterministic, so a cell built as part of Tocantins carries the same
-- fine_id it would anywhere.
--
-- fine_id/fparent_id are CHAR(5). A CHAR(4) code is always the 0.25° ERA5 grid; the width
-- difference is the type tag that keeps the two code spaces from ever being confused.

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS chirps_base_grid (
    fine_id     CHAR(5)          NOT NULL,
    fparent_id  CHAR(5)          NOT NULL,   -- 20x20 fine cells = a 1.0° block, 400 cells
    lat         DOUBLE PRECISION NOT NULL,   -- cell CENTER, -60..60
    lon         DOUBLE PRECISION NOT NULL,   -- cell CENTER, -180..180
    is_valid    BOOLEAN,                     -- NULL until a backfill reports coverage
    geom        GEOMETRY(Polygon, 4326) NOT NULL,   -- 0.05° square cell
    PRIMARY KEY (fine_id)
);

CREATE INDEX IF NOT EXISTS idx_chirps_grid_fparent ON chirps_base_grid (fparent_id);
CREATE INDEX IF NOT EXISTS idx_chirps_grid_geom    ON chirps_base_grid USING GIST (geom);
