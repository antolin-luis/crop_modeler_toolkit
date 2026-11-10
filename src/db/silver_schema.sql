-- Silver layer DDL (PLANNING.md §8.2, §8.4).
--
-- Kept separate from schema.sql on purpose: schema.sql is the grid DDL that ships with
-- the seed and runs at first-boot via /docker-entrypoint-initdb.d. These tables are
-- created by the transform_silver DAG instead, so an existing database picks them up
-- without a volume reset.

CREATE TABLE IF NOT EXISTS wth_base (
    parent_id      CHAR(4)  NOT NULL,
    child_id       CHAR(4)  NOT NULL,
    date           DATE     NOT NULL,
    tmax           REAL,                                    -- °C
    tmin           REAL,                                    -- °C
    precip         REAL,                                    -- mm/day
    srad           REAL,                                    -- MJ/m²/day
    wind           REAL,                                    -- m/s @ 10 m
    tdew           REAL,                                    -- °C
    rh             REAL,                                    -- %      (Tetens §12.1)
    et0            REAL,                                    -- mm/day (FAO-56 §12.2)
    is_preliminary BOOLEAN  NOT NULL DEFAULT TRUE,          -- ERA5T vs final (§11.3)
    ingested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (parent_id, child_id, date)                 -- partition key must be in the PK
) PARTITION BY LIST (parent_id);

-- The `date` in wth_base is a LOCAL calendar day. The offset that defined that 24-hour
-- window is per-cell and lives on the grid seed as era5_land_base_grid.t_zone (standard UTC
-- offset in minutes) — derived once from the political tz shapefile and applied at GEE
-- reduction time (§5.3). query.fetch_cell_series joins the grid to surface it. (Historic:
-- an earlier per-cell offset table stamped this from a manual transform param; the grid
-- column replaced it as the single source of truth.)

-- Rows rejected by the QA node (§8.4). Not partitioned: this stays small, and if it does
-- not, that is the signal to investigate rather than to scale the table.
CREATE TABLE IF NOT EXISTS wth_qa_failures (
    parent_id      CHAR(4)  NOT NULL,
    child_id       CHAR(4)  NOT NULL,
    date           DATE     NOT NULL,
    tmax           REAL,
    tmin           REAL,
    precip         REAL,
    srad           REAL,
    wind           REAL,
    tdew           REAL,
    rh             REAL,
    et0            REAL,
    reason         TEXT     NOT NULL,
    detected_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (parent_id, child_id, date)
);

-- Provenance for repaired values (§8.4). A bitmask, not a boolean: a cell-day can have one
-- variable imputed and the rest observed, and a consumer must be able to tell which. Bit
-- order follows the wth_base DDL column order.
--   tmax=1  tmin=2  precip=4  srad=8  wind=16  tdew=32  rh=64  et0=128
-- `is_preliminary` is deliberately NOT reused for this: it means ERA5T-vs-final source
-- state (§11.3), and overloading it would make both facts unreadable.
--
-- A non-volatile DEFAULT is metadata-only since PG 11, so this is not a table rewrite even
-- at 172M rows.
ALTER TABLE wth_base ADD COLUMN IF NOT EXISTS imputed SMALLINT NOT NULL DEFAULT 0;

-- Field-level QA findings (§8.4). Distinct from wth_qa_failures, which is a *cell-day*
-- quarantine: this is a registry of whole-field defects — one row per
-- (variable, date, detector) — that outlives any single incident and records what was
-- decided about it.
CREATE TABLE IF NOT EXISTS wth_data_issues (
    issue_id       BIGSERIAL PRIMARY KEY,
    variable       TEXT     NOT NULL,
    date           DATE     NOT NULL,
    detector       TEXT     NOT NULL,   -- constant_field | low_spread | climatology_outlier
    cells          INTEGER  NOT NULL,
    detail         JSONB    NOT NULL,   -- observed min/max/stddev (silver units), file, chunk
    status         TEXT     NOT NULL DEFAULT 'detected',
    -- detected            — found, not yet triaged
    -- refetch_pending     — repaired locally, but a clean source re-fetch is still owed
    -- refetched           — replaced from an independent source; not an estimate
    -- imputed             — filled from the repair ladder; see wth_imputation_log
    -- accepted_source_defect — no fix and no honest imputation exists; recorded as such
    -- false_positive      — detector fired on real data
    resolution     TEXT,
    detected_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at    TIMESTAMPTZ,
    UNIQUE (variable, date, detector)
);

-- One row per repaired cell-day-variable, so every correction is reversible when a fixed
-- upstream source appears. Deliberately holds the original value: without it a repair is
-- destructive.
CREATE TABLE IF NOT EXISTS wth_imputation_log (
    parent_id      CHAR(4)  NOT NULL,
    child_id       CHAR(4)  NOT NULL,
    date           DATE     NOT NULL,
    variable       TEXT     NOT NULL,
    method         TEXT     NOT NULL,   -- e.g. interpolate_temporal, analog_day(1994-05-16)
    original_value REAL,
    new_value      REAL,
    issue_id       BIGINT REFERENCES wth_data_issues(issue_id),
    applied_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (parent_id, child_id, date, variable)
);
