-- Alternative-precipitation silver DDL — CHIRPS on the 0.05° fine grid.
--
-- Created by the transform_precip_alt DAG, not shipped in the seed, so an existing database
-- picks it up without a volume reset (same reasoning as silver_schema.sql).
--
-- Deliberately a separate table from wth_base, not extra columns on it. wth_base is one
-- observation per cell-day on the 0.25° ERA5 grid, single-source by construction. CHIRPS is
-- a different grid, a different day definition, and multi-version. Forcing them together
-- would mean either resampling CHIRPS (losing the 25x detail that justifies it) or leaving
-- most of wth_base NULL.

-- === Day definition — read before joining anything to wth_base ===================
-- wth_base.date is a LOCAL day: the ERA5 backend reduces hourly values inside a 24h window
-- shifted by each cell's era5_land_base_grid.t_zone (PLANNING.md §5.3).
--
-- wth_precip_alt.date is the CHIRPS product day, a fixed UTC-anchored window. CHIRPS ships
-- already-daily, so there are no sub-daily values left to re-window and the offset cannot be
-- applied after the fact — daily reduction is lossy.
--
-- The two columns therefore denote slightly different 24-hour windows. That is a property of
-- the data, not a defect to correct. Do not join on date alone as though they agree; use the
-- precip_compare view, which states the offset it is ignoring.

CREATE TABLE IF NOT EXISTS wth_precip_alt (
    fparent_id  CHAR(5)  NOT NULL,
    fine_id     CHAR(5)  NOT NULL,
    date        DATE     NOT NULL,          -- CHIRPS product day; see the note above
    source      SMALLINT NOT NULL,          -- see wth_precip_alt_source
    precip      REAL,                       -- mm/day, native CHIRPS, unconverted
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (fparent_id, fine_id, date, source)   -- partition key must be in the PK
) PARTITION BY LIST (fparent_id);

-- `source` is SMALLINT rather than TEXT because it sits in the primary key of a table that
-- reaches ~10^8 rows at a single state's extent, so its width is paid in both heap and
-- index. This table keeps it legible.
CREATE TABLE IF NOT EXISTS wth_precip_alt_source (
    source      SMALLINT PRIMARY KEY,
    code        TEXT NOT NULL UNIQUE,       -- 'chirps_v2' | 'chirps_v3_rnl' | ...
    collection  TEXT NOT NULL,              -- the Earth Engine asset id
    note        TEXT
);

-- Rows rejected by the QA node. Not partitioned: this stays small, and if it does not, that
-- is the signal to investigate rather than to scale the table.
CREATE TABLE IF NOT EXISTS wth_precip_alt_qa_failures (
    fparent_id  CHAR(5)  NOT NULL,
    fine_id     CHAR(5)  NOT NULL,
    date        DATE     NOT NULL,
    source      SMALLINT NOT NULL,
    precip      REAL,
    reason      TEXT     NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (fparent_id, fine_id, date, source)
);

-- Area-weighted 0.05° -> 0.25° mapping. The two grids do not nest — ERA5 cell edges land on
-- 0.125 + k*0.25, which is not a multiple of 0.05 — so this is the ONLY correct way to move
-- between them. Populated per-extent by src/db/chirps_map.py. Weights sum to 1.0 per
-- (child_id, source-grid coverage).
CREATE TABLE IF NOT EXISTS chirps_era5_map (
    fine_id   CHAR(5) NOT NULL,
    child_id  CHAR(4) NOT NULL,
    weight    REAL    NOT NULL,
    PRIMARY KEY (fine_id, child_id)
);

CREATE INDEX IF NOT EXISTS idx_chirps_map_child ON chirps_era5_map (child_id);
