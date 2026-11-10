-- soil_profile_points / soil_era5_map DDL — the DSSAT soil layer.
--
-- Kept separate from schema.sql for the same reason silver_schema.sql and
-- fine_grid_schema.sql are: schema.sql ships with the grid seed and runs once at first
-- boot via /docker-entrypoint-initdb.d. These tables are created by the soil_grid_build
-- DAG instead, so an existing database picks them up with no volume reset.
--
-- Source: Point5m_SoilGrids-for-DSSAT-10km_v1.shp — a global 5 arc-min (~10 km) point
-- layer, 1,984,797 land points, one DSSAT-ready soil profile each. `soil_id` is the
-- source's `SoilProfil` attribute, which is DSSAT's ID_SOIL (the FILEX field), NOT a
-- filename: values look like 'AD02455938' = ISO2 + zero-padded CELL5M. The .SOL file that
-- holds the profile's layer data lives outside this database; only the identity is stored.
--
-- soil_id/iso2 are VARCHAR and deliberately not CHAR(n): psycopg2 hands CHAR(n) back
-- space-padded, and every consumer then has to remember .str.strip() (see the note in
-- silver_load.fetch_cell_meta). Nothing here is a fixed-width code space, so nothing here
-- pays that tax. The width is not asserted either — CELL5M is 5 to 7 digits depending on
-- latitude, so profile IDs are not all 10 characters.

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS soil_profile_points (
    cell5m      INTEGER          NOT NULL,   -- HarvestChoice 5 arc-min cell id (source PK)
    soil_id     VARCHAR(12)      NOT NULL,   -- DSSAT ID_SOIL, e.g. 'AD02455938'
    iso2        VARCHAR(4)       NOT NULL,   -- country; also names the .SOL file (<ISO2>.SOL)
    lat         DOUBLE PRECISION NOT NULL,   -- point, -90..90
    lon         DOUBLE PRECISION NOT NULL,   -- point, -180..180
    child_id    CHAR(4)          NOT NULL,   -- ERA5 0.25° cell containing the point
    parent_id   CHAR(4)          NOT NULL,
    geom        GEOMETRY(Point, 4326) NOT NULL,
    PRIMARY KEY (cell5m)
);

CREATE INDEX IF NOT EXISTS idx_soil_points_child ON soil_profile_points (child_id);
CREATE INDEX IF NOT EXISTS idx_soil_points_iso2  ON soil_profile_points (iso2);
CREATE INDEX IF NOT EXISTS idx_soil_points_geom  ON soil_profile_points USING GIST (geom);

-- The join that pairs a weather cell with a soil profile: one row per (ERA5 cell, soil
-- point) pair, with the point closest to the cell centre flagged. An 0.25° cell holds
-- ~9 of these 5 arc-min points, so `is_nearest` is what a "one .WTH, one ID_SOIL" caller
-- wants, while the full set stays available for anyone doing something smarter.
CREATE TABLE IF NOT EXISTS soil_era5_map (
    child_id    CHAR(4)     NOT NULL,
    cell5m      INTEGER     NOT NULL,
    soil_id     VARCHAR(12) NOT NULL,
    dist_deg    REAL        NOT NULL,   -- point -> cell centre, degrees, cos-lat scaled
    is_nearest  BOOLEAN     NOT NULL,
    PRIMARY KEY (child_id, cell5m)
);

CREATE INDEX IF NOT EXISTS idx_soil_map_nearest
    ON soil_era5_map (child_id) WHERE is_nearest;
