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
