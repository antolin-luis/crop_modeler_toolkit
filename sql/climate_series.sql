-- ===========================================================================
-- climate_series.sql — pull a daily weather time series for one coordinate.
--
-- General-purpose read API. No DSSAT, no file format, no year scoping: you get the
-- whole record unless you ask for less.
--
--   SELECT * FROM climate_series(-5.175, -50.725);                       -- everything
--   SELECT date, tmax, tmin, precip FROM climate_series(-5.175, -50.725);-- pick columns
--   SELECT * FROM climate_series(-5.175, -50.725, '2000-01-01', '2020-12-31');
--   SELECT * FROM climate_series(-5.175, -50.725, NULL, NULL, 'chirps_v2');
--
-- HOW TO IMPORT (DBeaver)
--   SQL Editor > Open SQL script... > this file > Execute script (Alt+X).
--   NOT Ctrl+Enter, which runs only the statement under the cursor.
--
-- HOW TO IMPORT (psql)
--   docker compose exec -T postgres psql -U era5 -d era5 -v ON_ERROR_STOP=1 \
--       -f - < sql/climate_series.sql
--
-- Self-contained on purpose: it re-declares the grid encoders so you can import this
-- file alone, without sql/wth_helpers.sql. The definitions are identical in both files,
-- and the verification query at the bottom checks them against the grid tables — so a
-- drift between the two would fail loudly at import rather than quietly at read time.
--
-- Read-only: functions only, no tables, no data touched. Safe to re-run.
-- ===========================================================================


-- ---------------------------------------------------------------------------
-- 1. Grid encoders (identical to sql/wth_helpers.sql §1-3)
--
--    A coordinate resolves to its cell arithmetically — no lookup table, no PostGIS,
--    no spatial index. geom + GiST is for polygon work only.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION base36(n BIGINT, width INT) RETURNS TEXT
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
  alph CONSTANT TEXT := '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ';
  s TEXT := '';
  r INT;
BEGIN
  FOR i IN 1..width LOOP
    r := n % 36;
    s := substr(alph, r + 1, 1) || s;
    n := n / 36;
  END LOOP;
  IF n <> 0 THEN
    RAISE EXCEPTION 'value does not fit in % base-36 chars', width;
  END IF;
  RETURN s;
END;
$$;

CREATE OR REPLACE FUNCTION base36_decode(code TEXT) RETURNS BIGINT
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
  alph CONSTANT TEXT := '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ';
  n BIGINT := 0;
  pos INT;
BEGIN
  FOR i IN 1..length(code) LOOP
    pos := strpos(alph, substr(code, i, 1));
    IF pos = 0 THEN
      RAISE EXCEPTION 'bad base-36 char in %', code;
    END IF;
    n := n * 36 + (pos - 1);
  END LOOP;
  RETURN n;
END;
$$;

-- Python's round() is banker's rounding; Postgres round() is half-away-from-zero. They
-- differ only for a coordinate exactly on a 0.25 deg cell boundary (x.125) — and when
-- they differ they pick a DIFFERENT CELL. The database was built with the Python rule.
CREATE OR REPLACE FUNCTION round_half_even(x NUMERIC) RETURNS BIGINT
LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE
    WHEN x - floor(x) = 0.5 THEN
      CASE WHEN floor(x)::BIGINT % 2 = 0 THEN floor(x)::BIGINT ELSE floor(x)::BIGINT + 1 END
    ELSE round(x)::BIGINT
  END
$$;

-- 0.25 deg ERA5 grid: cell CENTRES sit on multiples of 0.25, so bin with round().
-- b = 4 is the parent block size the database was built with. IMMUTABLE.
CREATE OR REPLACE FUNCTION era5_lat_idx(lat DOUBLE PRECISION) RETURNS BIGINT
LANGUAGE sql IMMUTABLE AS $$ SELECT round_half_even((90.0 - lat::numeric) / 0.25) $$;

CREATE OR REPLACE FUNCTION era5_lon_idx(lon DOUBLE PRECISION) RETURNS BIGINT
LANGUAGE sql IMMUTABLE AS $$
  SELECT round_half_even(mod(mod(lon::numeric, 360) + 360, 360) / 0.25)
$$;

CREATE OR REPLACE FUNCTION era5_child_id(lat DOUBLE PRECISION, lon DOUBLE PRECISION)
RETURNS CHAR(4) LANGUAGE sql IMMUTABLE AS $$
  SELECT base36(era5_lat_idx(lat) * 1440 + era5_lon_idx(lon), 4)::CHAR(4)
$$;

CREATE OR REPLACE FUNCTION era5_parent_id(lat DOUBLE PRECISION, lon DOUBLE PRECISION, b INT DEFAULT 4)
RETURNS CHAR(4) LANGUAGE sql IMMUTABLE AS $$
  SELECT base36((era5_lat_idx(lat) / b) * ((1440 + b - 1) / b) + (era5_lon_idx(lon) / b), 4)::CHAR(4)
$$;

CREATE OR REPLACE FUNCTION era5_parent_of(child_id TEXT, b INT DEFAULT 4) RETURNS CHAR(4)
LANGUAGE sql IMMUTABLE AS $$
  SELECT base36(((base36_decode(rtrim(child_id)) / 1440) / b) * ((1440 + b - 1) / b)
                + ((base36_decode(rtrim(child_id)) % 1440) / b), 4)::CHAR(4)
$$;

-- 0.05 deg CHIRPS fine grid: cell EDGES sit on multiples of 0.05, so bin with floor().
-- Multiply by 20 rather than divide by 0.05 — 0.05 has no exact binary form, and dividing
-- puts edge-aligned coordinates on the wrong side of the floor about half the time.
CREATE OR REPLACE FUNCTION chirps_lat_idx(lat DOUBLE PRECISION) RETURNS BIGINT
LANGUAGE sql IMMUTABLE AS $$
  SELECT least(greatest(floor((60.0 - lat) * 20 + 1e-9)::BIGINT, 0), 2399)
$$;

CREATE OR REPLACE FUNCTION chirps_lon_idx(lon DOUBLE PRECISION) RETURNS BIGINT
LANGUAGE sql IMMUTABLE AS $$
  SELECT least(greatest(floor(mod(mod(lon::numeric, 360) + 360, 360) * 20 + 1e-9)::BIGINT, 0), 7199)
$$;

CREATE OR REPLACE FUNCTION chirps_fine_id(lat DOUBLE PRECISION, lon DOUBLE PRECISION)
RETURNS CHAR(5) LANGUAGE sql IMMUTABLE AS $$
  SELECT base36(chirps_lat_idx(lat) * 7200 + chirps_lon_idx(lon), 5)::CHAR(5)
$$;

CREATE OR REPLACE FUNCTION chirps_fparent_id(lat DOUBLE PRECISION, lon DOUBLE PRECISION)
RETURNS CHAR(5) LANGUAGE sql IMMUTABLE AS $$
  SELECT base36((chirps_lat_idx(lat) / 20) * 360 + (chirps_lon_idx(lon) / 20), 5)::CHAR(5)
$$;

CREATE OR REPLACE FUNCTION chirps_fparent_of(fine_id TEXT) RETURNS CHAR(5)
LANGUAGE sql IMMUTABLE AS $$
  SELECT base36(((base36_decode(rtrim(fine_id)) / 7200) / 20) * 360
                + ((base36_decode(rtrim(fine_id)) % 7200) / 20), 5)::CHAR(5)
$$;


-- ---------------------------------------------------------------------------
-- 2. Rainfall source
--
--   'era5'          wth_base.precip — the cell's own LOCAL-day total
--   'chirps_v2'     UCSB-CHG/CHIRPS/DAILY        (the long-standing reference product)
--   'chirps_v3_rnl' UCSB-CHC/CHIRPS/V3/DAILY_RNL (pentad totals disaggregated by ERA5,
--                                                 so daily structure is derived)
--   'chirps_v3_sat' UCSB-CHC/CHIRPS/V3/DAILY_SAT (near-real-time)
--
-- *** DAY DEFINITION — read before mixing sources ***
-- wth_base.date is the cell's LOCAL day (window shifted by era5_land_base_grid.t_zone).
-- wth_precip_alt.date is the CHIRPS PRODUCT day, a fixed UTC-anchored window. Joining on
-- date treats two slightly different 24-hour windows as equal. Fine for monthly and
-- seasonal totals; a single-day difference between sources is NOT a measurement
-- disagreement. Daily reduction is lossy, so this cannot be fixed after the fact.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION climate_rain_source_code(src TEXT) RETURNS SMALLINT
LANGUAGE plpgsql STABLE AS $$
DECLARE
  code SMALLINT;
BEGIN
  IF src IS NULL OR lower(src) = 'era5' THEN
    RETURN NULL;                       -- NULL means "use wth_base.precip"
  END IF;
  SELECT s.source INTO code FROM wth_precip_alt_source s WHERE s.code = lower(src);
  IF code IS NULL THEN
    RAISE EXCEPTION 'unknown rain source %; known: era5, %',
      src, (SELECT string_agg(s.code, ', ' ORDER BY s.source) FROM wth_precip_alt_source s);
  END IF;
  RETURN code;
END;
$$;


-- ---------------------------------------------------------------------------
-- 3. climate_series — the whole point of this file
--
--   p_start / p_end are OPTIONAL. NULL means open-ended, so both NULL returns the entire
--   record for that cell (1952-2026 in this database, ~17k rows for one cell).
--
--   Columns are silver units, unconverted:
--     tmax tmin tdew  degC        precip  mm/day     srad  MJ/m2/day
--     wind  m/s AT 10 m           rh      %          et0   mm/day (FAO-56)
--   For DSSAT wind in km/day at 2 m: wind * 0.748 * 86.4. Not done here — this is the
--   general read API, and silver units are what the rest of the project speaks.
--
--   To select variables, select columns: SELECT date, tmax, precip FROM climate_series(...).
--   That is what SQL is for; there is no p_vars argument on this function.
--
--   p_chirps_mode:
--     'point'    one 0.05 deg cell (~5.5 km) containing the coordinate
--     'weighted' area-weighted mean of every fine cell overlapping the 0.25 deg cell.
--                Needs chirps_era5_map: uv run python -m src.db.chirps_map
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION climate_series(
    p_lat  DOUBLE PRECISION,
    p_lon  DOUBLE PRECISION,
    p_start DATE DEFAULT NULL,
    p_end   DATE DEFAULT NULL,
    p_rain  TEXT DEFAULT 'era5',
    p_chirps_mode TEXT DEFAULT 'point'
)
RETURNS TABLE (
    date           DATE,
    tmax           REAL,      -- degC
    tmin           REAL,      -- degC
    precip         REAL,      -- mm/day, from p_rain
    srad           REAL,      -- MJ/m2/day
    wind           REAL,      -- m/s at 10 m
    tdew           REAL,      -- degC
    rh             REAL,      -- %
    et0            REAL,      -- mm/day
    rain_coverage  REAL,      -- 1.0 unless chirps_mode='weighted' and the fine grid is partial
    is_preliminary BOOLEAN,   -- ERA5T rather than final ERA5
    imputed        SMALLINT   -- repair bitmask: tmax=1 tmin=2 precip=4 srad=8 wind=16
                              --                 tdew=32 rh=64 et0=128
)
LANGUAGE plpgsql STABLE AS $$
DECLARE
  v_child   CHAR(4)  := era5_child_id(p_lat, p_lon);
  v_parent  CHAR(4)  := era5_parent_id(p_lat, p_lon, 4);
  v_fine    CHAR(5)  := chirps_fine_id(p_lat, p_lon);
  v_fparent CHAR(5)  := chirps_fparent_id(p_lat, p_lon);
  v_src     SMALLINT := climate_rain_source_code(p_rain);
BEGIN
  IF v_src IS NULL THEN                                    -- ERA5 rain
    RETURN QUERY
      SELECT w.date, w.tmax, w.tmin, w.precip, w.srad, w.wind, w.tdew, w.rh, w.et0,
             1.0::real, w.is_preliminary, w.imputed
      FROM wth_base w
      WHERE w.parent_id = v_parent AND w.child_id = v_child
        AND (p_start IS NULL OR w.date >= p_start)
        AND (p_end   IS NULL OR w.date <= p_end)
      ORDER BY w.date;

  ELSIF lower(p_chirps_mode) = 'point' THEN                -- CHIRPS, nearest fine cell
    RETURN QUERY
      WITH r AS (
        SELECT p.date AS d, p.precip
        FROM wth_precip_alt p
        WHERE p.fparent_id = v_fparent AND p.fine_id = v_fine AND p.source = v_src
          AND (p_start IS NULL OR p.date >= p_start)
          AND (p_end   IS NULL OR p.date <= p_end)
      )
      SELECT w.date, w.tmax, w.tmin, r.precip, w.srad, w.wind, w.tdew, w.rh, w.et0,
             CASE WHEN r.precip IS NULL THEN NULL ELSE 1.0::real END,
             w.is_preliminary, w.imputed
      FROM wth_base w
      LEFT JOIN r ON r.d = w.date          -- LEFT: a CHIRPS gap is NULL, not a dropped day
      WHERE w.parent_id = v_parent AND w.child_id = v_child
        AND (p_start IS NULL OR w.date >= p_start)
        AND (p_end   IS NULL OR w.date <= p_end)
      ORDER BY w.date;

  ELSIF lower(p_chirps_mode) = 'weighted' THEN             -- CHIRPS, area-weighted
    IF NOT EXISTS (SELECT 1 FROM chirps_era5_map m WHERE m.child_id = v_child) THEN
      RAISE EXCEPTION
        'chirps_era5_map has no rows for cell % — build it with: uv run python -m src.db.chirps_map',
        v_child;
    END IF;
    RETURN QUERY
      WITH fine AS (
        SELECT m.fine_id, chirps_fparent_of(m.fine_id::text) AS fparent_id, m.weight
        FROM chirps_era5_map m WHERE m.child_id = v_child
      ), r AS (
        SELECT p.date AS d,
               (sum(p.precip * f.weight) / nullif(sum(f.weight), 0))::real AS precip,
               sum(f.weight)::real AS coverage
        FROM fine f
        JOIN wth_precip_alt p
          ON p.fparent_id = f.fparent_id AND p.fine_id = f.fine_id AND p.source = v_src
        WHERE (p_start IS NULL OR p.date >= p_start)
          AND (p_end   IS NULL OR p.date <= p_end)
        GROUP BY p.date
      )
      SELECT w.date, w.tmax, w.tmin, r.precip, w.srad, w.wind, w.tdew, w.rh, w.et0,
             r.coverage, w.is_preliminary, w.imputed
      FROM wth_base w
      LEFT JOIN r ON r.d = w.date
      WHERE w.parent_id = v_parent AND w.child_id = v_child
        AND (p_start IS NULL OR w.date >= p_start)
        AND (p_end   IS NULL OR w.date <= p_end)
      ORDER BY w.date;

  ELSE
    RAISE EXCEPTION 'unknown chirps_mode %; use ''point'' or ''weighted''', p_chirps_mode;
  END IF;
END;
$$;


-- ---------------------------------------------------------------------------
-- 4. climate_series_long — the same data in (date, variable, value) form
--
--    For plotting libraries, faceting, or when the variable list is itself data. Pass
--    p_vars to subset; NULL means all eight. Meta columns (is_preliminary, imputed,
--    rain_coverage) are not emitted here — they describe the row, not a variable.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION climate_series_long(
    p_lat   DOUBLE PRECISION,
    p_lon   DOUBLE PRECISION,
    p_start DATE DEFAULT NULL,
    p_end   DATE DEFAULT NULL,
    p_rain  TEXT DEFAULT 'era5',
    p_vars  TEXT[] DEFAULT NULL,
    p_chirps_mode TEXT DEFAULT 'point'
)
RETURNS TABLE (date DATE, variable TEXT, value REAL)
LANGUAGE plpgsql STABLE AS $$
DECLARE
  known CONSTANT TEXT[] := ARRAY['tmax','tmin','precip','srad','wind','tdew','rh','et0'];
  want  TEXT[];
  bad   TEXT;
BEGIN
  want := COALESCE(p_vars, known);
  SELECT v INTO bad FROM unnest(want) v WHERE v <> ALL(known) LIMIT 1;
  IF bad IS NOT NULL THEN
    RAISE EXCEPTION 'unknown variable %; known: %', bad, array_to_string(known, ', ');
  END IF;

  RETURN QUERY
    SELECT s.date, u.variable, u.value
    FROM climate_series(p_lat, p_lon, p_start, p_end, p_rain, p_chirps_mode) s
    CROSS JOIN LATERAL (
      VALUES ('tmax', s.tmax), ('tmin', s.tmin), ('precip', s.precip), ('srad', s.srad),
             ('wind', s.wind), ('tdew', s.tdew), ('rh', s.rh), ('et0', s.et0)
    ) AS u(variable, value)
    WHERE u.variable = ANY(want)
    ORDER BY s.date, u.variable;
END;
$$;


-- ---------------------------------------------------------------------------
-- 5. climate_series_info — what record exists for this coordinate
--
--    Run this first. It answers "how far back does this cell go, and how much of it is
--    actually there" before you pull 17,000 rows and wonder about the gaps.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION climate_series_info(
    p_lat DOUBLE PRECISION,
    p_lon DOUBLE PRECISION
)
RETURNS TABLE (
    child_id    CHAR(4),     -- also the DSSAT station code
    parent_id   CHAR(4),     -- LIST partition key
    fine_id     CHAR(5),     -- the 0.05 deg CHIRPS cell
    cell_lat    DOUBLE PRECISION,   -- cell CENTRE, not the requested coordinate
    cell_lon    DOUBLE PRECISION,
    elevation   REAL,        -- m, from geopotential (z / 9.80665)
    t_zone      SMALLINT,    -- the cell's local-day offset, minutes
    is_land     BOOLEAN,
    first_date  DATE,
    last_date   DATE,
    n_days      BIGINT,      -- rows present
    n_expected  BIGINT,      -- calendar days between first and last
    n_missing   BIGINT,      -- the gap, if any
    n_prelim    BIGINT,      -- preliminary ERA5T rows
    n_imputed   BIGINT       -- repaired rows (any variable)
)
LANGUAGE plpgsql STABLE AS $$
DECLARE
  v_child  CHAR(4) := era5_child_id(p_lat, p_lon);
  v_parent CHAR(4) := era5_parent_id(p_lat, p_lon, 4);
BEGIN
  RETURN QUERY
    WITH s AS (
      SELECT min(w.date) AS d0, max(w.date) AS d1, count(*) AS n,
             count(*) FILTER (WHERE w.is_preliminary) AS n_prelim,
             count(*) FILTER (WHERE w.imputed <> 0)   AS n_imputed
      FROM wth_base w
      WHERE w.parent_id = v_parent AND w.child_id = v_child
    )
    -- The fine cell of the REQUESTED coordinate, not of the 0.25 deg cell centre: that is
    -- the cell climate_series() reads CHIRPS from, and the two differ for most inputs
    -- (the centre is up to 0.125 deg away, which is 2-3 fine cells).
    SELECT g.child_id, g.parent_id, chirps_fine_id(p_lat, p_lon),
           g.lat, g.lon, g.elevation, g.t_zone, g.is_land,
           s.d0, s.d1, s.n,
           CASE WHEN s.d0 IS NULL THEN 0 ELSE (s.d1 - s.d0 + 1)::bigint END,
           CASE WHEN s.d0 IS NULL THEN 0 ELSE (s.d1 - s.d0 + 1)::bigint - s.n END,
           s.n_prelim, s.n_imputed
    FROM era5_land_base_grid g CROSS JOIN s
    WHERE g.child_id = v_child;
END;
$$;


-- ---------------------------------------------------------------------------
-- 6. climate_series_missing — the exact days with no row, inside a window
--
--    Defaults to the cell's own first/last date, so with no arguments it reports gaps
--    inside the record rather than "missing" days before the record starts.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION climate_series_missing(
    p_lat   DOUBLE PRECISION,
    p_lon   DOUBLE PRECISION,
    p_start DATE DEFAULT NULL,
    p_end   DATE DEFAULT NULL
)
RETURNS TABLE (missing_date DATE)
LANGUAGE plpgsql STABLE AS $$
DECLARE
  v_child  CHAR(4) := era5_child_id(p_lat, p_lon);
  v_parent CHAR(4) := era5_parent_id(p_lat, p_lon, 4);
  v_d0 DATE;
  v_d1 DATE;
BEGIN
  SELECT COALESCE(p_start, min(w.date)), COALESCE(p_end, max(w.date))
    INTO v_d0, v_d1
  FROM wth_base w
  WHERE w.parent_id = v_parent AND w.child_id = v_child;

  IF v_d0 IS NULL THEN
    RETURN;                      -- no record at all for this cell; nothing to call missing
  END IF;

  RETURN QUERY
    SELECT d::date
    FROM generate_series(v_d0, v_d1, interval '1 day') d
    WHERE NOT EXISTS (
      SELECT 1 FROM wth_base w
      WHERE w.parent_id = v_parent AND w.child_id = v_child AND w.date = d::date)
    ORDER BY 1;
END;
$$;


-- ---------------------------------------------------------------------------
-- 7. climate_monthly — monthly aggregate of the same series
--
--    Precipitation sums; everything else averages. This is also the right resolution at
--    which to compare ERA5 against CHIRPS: it is coarse enough that the day-window
--    mismatch between the two stops mattering.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION climate_monthly(
    p_lat   DOUBLE PRECISION,
    p_lon   DOUBLE PRECISION,
    p_start DATE DEFAULT NULL,
    p_end   DATE DEFAULT NULL,
    p_rain  TEXT DEFAULT 'era5',
    p_chirps_mode TEXT DEFAULT 'point'
)
RETURNS TABLE (
    month      DATE,          -- first day of the month
    n_days     BIGINT,
    tmax_mean  DOUBLE PRECISION,
    tmin_mean  DOUBLE PRECISION,
    precip_sum DOUBLE PRECISION,
    srad_mean  DOUBLE PRECISION,
    wind_mean  DOUBLE PRECISION,
    rh_mean    DOUBLE PRECISION,
    et0_sum    DOUBLE PRECISION
)
LANGUAGE sql STABLE AS $$
  SELECT date_trunc('month', s.date)::date, count(*),
         avg(s.tmax), avg(s.tmin), sum(s.precip), avg(s.srad),
         avg(s.wind), avg(s.rh), sum(s.et0)
  FROM climate_series(p_lat, p_lon, p_start, p_end, p_rain, p_chirps_mode) s
  GROUP BY 1
  ORDER BY 1
$$;


-- ===========================================================================
-- 8. Verify the install. Every *_bad column must be 0 — these encoders must reproduce
--    the codes already stored in the grid tables, because the database was built with
--    the Python encoders in src/grid/.
-- ===========================================================================

SELECT count(*) AS checked,
       count(*) FILTER (WHERE era5_child_id(lat, lon)     <> child_id)  AS child_bad,
       count(*) FILTER (WHERE era5_parent_id(lat, lon, 4) <> parent_id) AS parent_bad
FROM (SELECT * FROM era5_land_base_grid TABLESAMPLE SYSTEM (1)) s;


-- ===========================================================================
-- 9. Usage
-- ===========================================================================

-- What is here? Run this before pulling anything.
--   SELECT * FROM climate_series_info(-5.175, -50.725);

-- The entire record, every variable:
--   SELECT * FROM climate_series(-5.175, -50.725);

-- Just the columns you want (this is how you "choose variables"):
--   SELECT date, tmax, tmin, precip FROM climate_series(-5.175, -50.725);

-- A date window (either end may be NULL for open-ended):
--   SELECT * FROM climate_series(-5.175, -50.725, '2000-01-01', '2020-12-31');
--   SELECT * FROM climate_series(-5.175, -50.725, '2015-01-01', NULL);

-- CHIRPS rainfall instead of ERA5, whole record:
--   SELECT * FROM climate_series(-5.175, -50.725, NULL, NULL, 'chirps_v2');
--   SELECT * FROM climate_series(-5.175, -50.725, NULL, NULL, 'chirps_v3_rnl');

-- CHIRPS area-weighted over the whole 0.25 deg cell (needs chirps_era5_map):
--   SELECT * FROM climate_series(-5.175, -50.725, NULL, NULL, 'chirps_v2', 'weighted');

-- Long format, two variables only:
--   SELECT * FROM climate_series_long(-5.175, -50.725, '2020-01-01', '2020-12-31',
--                                     'era5', ARRAY['tmax','precip']);

-- Three rainfall sources side by side, monthly (the honest resolution for comparing them):
--   SELECT e.month, e.precip_sum AS era5, v2.precip_sum AS chirps_v2, v3.precip_sum AS chirps_v3
--   FROM climate_monthly(-5.175, -50.725, '2020-01-01', '2020-12-31') e
--   JOIN climate_monthly(-5.175, -50.725, '2020-01-01', '2020-12-31', 'chirps_v2')     v2 USING (month)
--   JOIN climate_monthly(-5.175, -50.725, '2020-01-01', '2020-12-31', 'chirps_v3_rnl') v3 USING (month)
--   ORDER BY e.month;

-- Gaps inside the record:
--   SELECT * FROM climate_series_missing(-5.175, -50.725);

-- Export to CSV from psql (DBeaver: right-click the grid > Export resultset > CSV):
--   docker compose exec -T postgres psql -U era5 -d era5 \
--     -c "\copy (SELECT * FROM climate_series(-5.175,-50.725)) TO STDOUT WITH CSV HEADER" \
--     > series.csv


-- ===========================================================================
-- 10. Teardown (uncomment to remove what this file created; the grid encoders are
--     shared with sql/wth_helpers.sql, so they are NOT dropped here)
-- ===========================================================================
-- DROP FUNCTION IF EXISTS climate_series(double precision, double precision, date, date, text, text);
-- DROP FUNCTION IF EXISTS climate_series_long(double precision, double precision, date, date, text, text[], text);
-- DROP FUNCTION IF EXISTS climate_series_info(double precision, double precision);
-- DROP FUNCTION IF EXISTS climate_series_missing(double precision, double precision, date, date);
-- DROP FUNCTION IF EXISTS climate_monthly(double precision, double precision, date, date, text, text);
-- DROP FUNCTION IF EXISTS climate_rain_source_code(text);
