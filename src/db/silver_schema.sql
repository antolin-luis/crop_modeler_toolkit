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

-- Per-cell local-day offset (§5.3). The `date` in wth_base is a LOCAL calendar day, and
-- the offset that defined that day is a per-region download choice — not derivable from
-- longitude. Once regions with different offsets share wth_base, this table is what says
-- which 24-hour window a given cell's `date` means. child_id grain (a parent is always one
-- region, so this is finer than strictly needed, but it matches wth_base's own key and is
-- unambiguous). Populated by transform_silver from its `timezone` param; ON CONFLICT keeps
-- the latest offset if a cell is ever re-loaded under a different one.
CREATE TABLE IF NOT EXISTS cell_timezone (
    child_id           CHAR(4)     PRIMARY KEY,
    utc_offset_minutes SMALLINT    NOT NULL,   -- e.g. UTC-03:00 -> -180, UTC-06:00 -> -360
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

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
