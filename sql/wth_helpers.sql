-- ===========================================================================
-- wth_helpers.sql — build DSSAT .WTH files straight from the silver database.
--
-- Companion to docs/wth_from_sql.md, which explains every formula and caveat here.
-- This file is the runnable version: import it once, then call the functions.
--
-- HOW TO IMPORT (DBeaver)
--   1. Connect to the era5 database (host localhost, port 5432, db era5, user era5).
--   2. SQL Editor > Open SQL script... > pick this file.
--   3. Execute script (Alt+X) — NOT "execute statement" (Ctrl+Enter), which runs
--      only the statement under the cursor.
--   4. Verify with the SELECTs at the bottom of this file.
--
-- HOW TO IMPORT (psql)
--   docker compose exec -T postgres psql -U era5 -d era5 -v ON_ERROR_STOP=1 \
--       -f - < sql/wth_helpers.sql
--
-- Everything here is read-only: functions only, no tables, no data touched. Safe to
-- run on a live database, and safe to re-run (every function is CREATE OR REPLACE).
-- Teardown is at the very bottom, commented out.
--
-- No psql backslash commands are used anywhere, so DBeaver, pgAdmin, DataGrip and
-- psql all import this file identically.
-- ===========================================================================


-- ---------------------------------------------------------------------------
-- 1. base-36 codec
--    The alphabet is the canonical one from src/grid/spec.py. Do not change it.
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

-- Python's round() is banker's rounding (ties to even); Postgres round() is
-- half-away-from-zero. They differ only for a coordinate landing exactly on a 0.25 deg
-- cell boundary (x.125) — but when they differ they pick a DIFFERENT CELL, and the
-- database was built with the Python rule. Arguments here are always >= 0.
CREATE OR REPLACE FUNCTION round_half_even(x NUMERIC) RETURNS BIGINT
LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE
    WHEN x - floor(x) = 0.5 THEN
      CASE WHEN floor(x)::BIGINT % 2 = 0 THEN floor(x)::BIGINT ELSE floor(x)::BIGINT + 1 END
    ELSE round(x)::BIGINT
  END
$$;


-- ---------------------------------------------------------------------------
-- 2. 0.25 deg ERA5 grid (src/grid/encoding.py)
--    Cell CENTRES sit on multiples of 0.25, so bin with round().
--    b = 4 is the parent block size the database was built with. IMMUTABLE — it is
--    baked into every stored parent_id and every partition name.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION era5_lat_idx(lat DOUBLE PRECISION) RETURNS BIGINT
LANGUAGE sql IMMUTABLE AS $$
  SELECT round_half_even((90.0 - lat::numeric) / 0.25)
$$;

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

-- Parent straight from a child code — no coordinates needed. This is what lets a region
-- query put the LIST partition key in the WHERE clause without a second round-trip.
CREATE OR REPLACE FUNCTION era5_parent_of(child_id TEXT, b INT DEFAULT 4) RETURNS CHAR(4)
LANGUAGE sql IMMUTABLE AS $$
  SELECT base36(((base36_decode(rtrim(child_id)) / 1440) / b) * ((1440 + b - 1) / b)
                + ((base36_decode(rtrim(child_id)) % 1440) / b), 4)::CHAR(4)
$$;

CREATE OR REPLACE FUNCTION era5_cell_center(lat DOUBLE PRECISION, lon DOUBLE PRECISION,
                                            OUT clat DOUBLE PRECISION, OUT clon DOUBLE PRECISION)
LANGUAGE sql IMMUTABLE AS $$
  SELECT 90.0 - era5_lat_idx(lat) * 0.25,
         CASE WHEN era5_lon_idx(lon) * 0.25 > 180.0
              THEN era5_lon_idx(lon) * 0.25 - 360.0
              ELSE era5_lon_idx(lon) * 0.25 END
$$;


-- ---------------------------------------------------------------------------
-- 3. 0.05 deg CHIRPS fine grid (src/grid/fine_encoding.py)
--    Cell EDGES sit on multiples of 0.05, so bin with floor(). Multiply by 20 rather
--    than divide by 0.05: 0.05 has no exact binary form, and dividing puts edge-aligned
--    coordinates on the wrong side of the floor about half the time.
--    Codes are 5 chars wide; a 4-char code is ALWAYS the 0.25 deg grid.
-- ---------------------------------------------------------------------------

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
-- 4. .WTH field formatting
--    DSSAT reads by column position in several modules, so widths are not cosmetic.
--    Missing is -99, never blank and never NULL.
--    FM strips to_char's padding; 99990.0 forces one decimal and a leading zero.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION wth_f6(x DOUBLE PRECISION) RETURNS TEXT
LANGUAGE sql IMMUTABLE AS $$
  SELECT lpad(to_char(COALESCE(x, -99)::numeric, 'FM99990.0'), 6)
$$;

-- Wind: silver holds m/s at 10 m. DSSAT wants km/day at 2 m.
--   0.748 = FAO-56 10 m -> 2 m factor;  86.4 = m/s -> km/day.
-- Because the 2 m adjustment happens HERE, the header's WNDHT must say 2.0, not 10.0.
CREATE OR REPLACE FUNCTION wth_wind_kmday(wind_ms10 DOUBLE PRECISION) RETURNS DOUBLE PRECISION
LANGUAGE sql IMMUTABLE AS $$
  SELECT wind_ms10 * 0.748 * 86.4
$$;

CREATE OR REPLACE FUNCTION wth_body_header_line() RETURNS TEXT
LANGUAGE sql IMMUTABLE AS $$
  SELECT '@DATE  SRAD  TMAX  TMIN  RAIN  DEWP  WIND   PAR  RHUM'
$$;

CREATE OR REPLACE FUNCTION wth_station_header_line() RETURNS TEXT
LANGUAGE sql IMMUTABLE AS $$
  SELECT '@ INSI      LAT     LONG  ELEV   TAV   AMP REFHT WNDHT'
$$;


-- ---------------------------------------------------------------------------
-- 5. Precipitation source resolution
--    'era5'          -> wth_base.precip, the cell's own local-day total
--    'chirps_v2'     -> UCSB-CHG/CHIRPS/DAILY        (the reference product)
--    'chirps_v3_rnl' -> UCSB-CHC/CHIRPS/V3/DAILY_RNL (pentad totals disaggregated by ERA5)
--    'chirps_v3_sat' -> UCSB-CHC/CHIRPS/V3/DAILY_SAT (near-real-time)
--
--  *** DAY DEFINITION — the one thing to understand before mixing sources ***
--  wth_base.date is the cell's LOCAL day (window shifted by era5_land_base_grid.t_zone).
--  wth_precip_alt.date is the CHIRPS PRODUCT day, a fixed UTC-anchored window. Joining on
--  date treats two different 24-hour windows as equal. Sound at monthly/seasonal totals;
--  a single-day difference is NOT a measurement disagreement. Daily reduction is lossy, so
--  this cannot be corrected after the fact.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION wth_rain_source_code(src TEXT) RETURNS SMALLINT
LANGUAGE plpgsql STABLE AS $$
DECLARE
  code SMALLINT;
BEGIN
  IF lower(src) = 'era5' THEN
    RETURN NULL;                      -- NULL means "use wth_base.precip"
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
-- 6. wth_body — the daily rows, as typed columns
--
--    chirps_mode:
--      'point'    one 0.05 deg cell (~5.5 km) containing the coordinate. What a point
--                 site usually wants.
--      'weighted' area-weighted mean of every fine cell overlapping the 0.25 deg cell.
--                 The correct aggregate when the file represents the whole cell. Needs
--                 chirps_era5_map, which is built per-extent:
--                     uv run python -m src.db.chirps_map
--                 The two grids do not nest (ERA5 edges land on 0.125 + k*0.25, not a
--                 multiple of 0.05), so this is a stored spatial join, never arithmetic.
--
--    A CHIRPS gap becomes -99 rather than a dropped day: the LEFT JOIN is deliberate.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION wth_body(
    p_lat  DOUBLE PRECISION,
    p_lon  DOUBLE PRECISION,
    p_year INT,
    p_rain_source TEXT DEFAULT 'era5',
    p_chirps_mode TEXT DEFAULT 'point'
)
RETURNS TABLE (
    date DATE,
    srad DOUBLE PRECISION,   -- MJ/m2/day
    tmax DOUBLE PRECISION,   -- degC
    tmin DOUBLE PRECISION,   -- degC
    rain DOUBLE PRECISION,   -- mm/day
    dewp DOUBLE PRECISION,   -- degC
    wind DOUBLE PRECISION,   -- km/day at 2 m (already converted)
    rhum DOUBLE PRECISION,   -- %
    rain_coverage DOUBLE PRECISION,  -- 1.0 for era5 / point; fine-grid coverage for weighted
    is_preliminary BOOLEAN,          -- ERA5T rather than final ERA5
    imputed SMALLINT                 -- repair bitmask; see wth_qa() notes
)
LANGUAGE plpgsql STABLE AS $$
DECLARE
  v_child   CHAR(4)  := era5_child_id(p_lat, p_lon);
  v_parent  CHAR(4)  := era5_parent_id(p_lat, p_lon, 4);
  v_fine    CHAR(5)  := chirps_fine_id(p_lat, p_lon);
  v_fparent CHAR(5)  := chirps_fparent_id(p_lat, p_lon);
  v_src     SMALLINT := wth_rain_source_code(p_rain_source);
  v_d0      DATE     := make_date(p_year, 1, 1);
  v_d1      DATE     := make_date(p_year, 12, 31);
BEGIN
  IF v_src IS NULL THEN                                   -- ERA5 rain
    RETURN QUERY
      SELECT w.date, w.srad::double precision, w.tmax::double precision,
             w.tmin::double precision, w.precip::double precision,
             w.tdew::double precision, wth_wind_kmday(w.wind::double precision),
             w.rh::double precision, 1.0::double precision,
             w.is_preliminary, w.imputed
      FROM wth_base w
      WHERE w.parent_id = v_parent AND w.child_id = v_child
        AND w.date BETWEEN v_d0 AND v_d1
      ORDER BY w.date;

  ELSIF lower(p_chirps_mode) = 'point' THEN               -- CHIRPS, nearest fine cell
    RETURN QUERY
      WITH r AS (
        SELECT p.date AS d, p.precip::double precision AS precip
        FROM wth_precip_alt p
        WHERE p.fparent_id = v_fparent AND p.fine_id = v_fine AND p.source = v_src
          AND p.date BETWEEN v_d0 AND v_d1
      )
      SELECT w.date, w.srad::double precision, w.tmax::double precision,
             w.tmin::double precision, r.precip,
             w.tdew::double precision, wth_wind_kmday(w.wind::double precision),
             w.rh::double precision,
             CASE WHEN r.precip IS NULL THEN NULL ELSE 1.0::double precision END,
             w.is_preliminary, w.imputed
      FROM wth_base w
      LEFT JOIN r ON r.d = w.date
      WHERE w.parent_id = v_parent AND w.child_id = v_child
        AND w.date BETWEEN v_d0 AND v_d1
      ORDER BY w.date;

  ELSIF lower(p_chirps_mode) = 'weighted' THEN            -- CHIRPS, area-weighted to the cell
    IF NOT EXISTS (SELECT 1 FROM chirps_era5_map m WHERE m.child_id = v_child) THEN
      RAISE EXCEPTION
        'chirps_era5_map has no rows for cell % — build it with: uv run python -m src.db.chirps_map',
        v_child;
    END IF;
    RETURN QUERY
      WITH fine AS (
        SELECT m.fine_id, chirps_fparent_of(m.fine_id::text) AS fparent_id, m.weight
        FROM chirps_era5_map m
        WHERE m.child_id = v_child
      ), r AS (
        SELECT p.date AS d,
               sum(p.precip * f.weight) / nullif(sum(f.weight), 0) AS precip,
               sum(f.weight) AS coverage
        FROM fine f
        JOIN wth_precip_alt p
          ON p.fparent_id = f.fparent_id AND p.fine_id = f.fine_id AND p.source = v_src
        WHERE p.date BETWEEN v_d0 AND v_d1
        GROUP BY p.date
      )
      SELECT w.date, w.srad::double precision, w.tmax::double precision,
             w.tmin::double precision, r.precip::double precision,
             w.tdew::double precision, wth_wind_kmday(w.wind::double precision),
             w.rh::double precision, r.coverage::double precision,
             w.is_preliminary, w.imputed
      FROM wth_base w
      LEFT JOIN r ON r.d = w.date
      WHERE w.parent_id = v_parent AND w.child_id = v_child
        AND w.date BETWEEN v_d0 AND v_d1
      ORDER BY w.date;

  ELSE
    RAISE EXCEPTION 'unknown chirps_mode %; use ''point'' or ''weighted''', p_chirps_mode;
  END IF;
END;
$$;


-- ---------------------------------------------------------------------------
-- 7. wth_header — the station line, as typed columns
--
--    ELEV comes from the grid seed, derived from geopotential (z / 9.80665). It is the
--    0.25 deg model orography, NOT a high-res DEM — silver's ET0 was computed against
--    it, and substituting a DEM makes the two inconsistent.
--
--    TAV = long-term mean air temperature.
--    AMP = HALF the annual range of the monthly means (DSSAT's annual amplitude). Some
--          tooling in the wild uses the full range instead. A 2x error in AMP shifts soil
--          temperature, and therefore emergence and phenology. Pick one and say so.
--
--    Default climatology window is the 1991-2020 WMO normal, not the simulated year, so
--    the header describes the SITE rather than that one year.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION wth_header(
    p_lat DOUBLE PRECISION,
    p_lon DOUBLE PRECISION,
    p_clim_from INT DEFAULT 1991,
    p_clim_to   INT DEFAULT 2020
)
RETURNS TABLE (
    insi      CHAR(4),
    child_id  CHAR(4),
    parent_id CHAR(4),
    lat       DOUBLE PRECISION,   -- cell CENTRE, not the requested coordinate
    lon       DOUBLE PRECISION,
    elev      DOUBLE PRECISION,   -- m
    tav       DOUBLE PRECISION,   -- degC
    amp       DOUBLE PRECISION,   -- degC
    refht     DOUBLE PRECISION,   -- m, temperature/humidity reference height
    wndht     DOUBLE PRECISION,   -- m, 2.0 because wth_wind_kmday already did 10 m -> 2 m
    t_zone    SMALLINT,           -- the cell's local-day offset, minutes
    n_months  INT                 -- months of climatology found; < 12 means a thin TAV/AMP
)
LANGUAGE plpgsql STABLE AS $$
DECLARE
  v_child  CHAR(4) := era5_child_id(p_lat, p_lon);
  v_parent CHAR(4) := era5_parent_id(p_lat, p_lon, 4);
BEGIN
  RETURN QUERY
    WITH monthly AS (
      SELECT date_part('month', w.date) AS mon, avg((w.tmax + w.tmin) / 2.0) AS tmean
      FROM wth_base w
      WHERE w.parent_id = v_parent AND w.child_id = v_child
        AND w.date BETWEEN make_date(p_clim_from, 1, 1) AND make_date(p_clim_to, 12, 31)
      GROUP BY 1
    ), clim AS (
      SELECT avg(tmean) AS tav, (max(tmean) - min(tmean)) / 2.0 AS amp, count(*)::int AS n
      FROM monthly
    )
    SELECT g.child_id, g.child_id, g.parent_id, g.lat, g.lon,
           g.elevation::double precision, k.tav, k.amp,
           2.0::double precision, 2.0::double precision, g.t_zone, k.n
    FROM era5_land_base_grid g CROSS JOIN clim k
    WHERE g.child_id = v_child;
END;
$$;


-- ---------------------------------------------------------------------------
-- 8. wth_header_lines / wth_body_lines / wth_file — the formatted text
--
--    wth_file returns the complete file, one row per line. Dump it with psql -qtA, or
--    in DBeaver export the single column as plain text with no header and no delimiter.
--    Filename convention: <child_id><year>.WTH — the station code IS the child_id, so a
--    file decodes straight back to a coordinate with no lookup table.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION wth_header_lines(
    p_lat DOUBLE PRECISION,
    p_lon DOUBLE PRECISION,
    p_year INT,
    p_rain_source TEXT DEFAULT 'era5',
    p_clim_from INT DEFAULT 1991,
    p_clim_to   INT DEFAULT 2020
)
RETURNS SETOF TEXT
LANGUAGE plpgsql STABLE AS $$
DECLARE
  h RECORD;
  descr TEXT;
BEGIN
  SELECT * INTO h FROM wth_header(p_lat, p_lon, p_clim_from, p_clim_to);
  IF h IS NULL THEN
    RAISE EXCEPTION 'no grid cell for (%, %) — outside the seeded grid?', p_lat, p_lon;
  END IF;

  descr := h.insi || ' ERA5 0.25deg local-day';
  IF lower(p_rain_source) <> 'era5' THEN
    descr := descr || ' + ' || lower(p_rain_source) || ' RAIN (product day)';
  END IF;
  descr := descr || ' ' || p_year;

  RETURN NEXT '*WEATHER DATA : ' || descr;
  RETURN NEXT '';
  RETURN NEXT wth_station_header_line();
  RETURN NEXT '  ' || h.insi
           || lpad(to_char(h.lat::numeric, 'FM9990.000'), 9)
           || lpad(to_char(h.lon::numeric, 'FM9990.000'), 9)
           || lpad(to_char(COALESCE(h.elev, -99)::numeric, 'FM99990'), 6)
           || wth_f6(h.tav)
           || wth_f6(h.amp)
           || wth_f6(h.refht)
           || wth_f6(h.wndht);
END;
$$;

CREATE OR REPLACE FUNCTION wth_body_lines(
    p_lat DOUBLE PRECISION,
    p_lon DOUBLE PRECISION,
    p_year INT,
    p_rain_source TEXT DEFAULT 'era5',
    p_chirps_mode TEXT DEFAULT 'point'
)
RETURNS SETOF TEXT
LANGUAGE sql STABLE AS $$
  SELECT to_char(b.date, 'YYDDD')   -- 2-digit year + day of year; the year lives in the filename
      || wth_f6(b.srad)
      || wth_f6(b.tmax)
      || wth_f6(b.tmin)
      || wth_f6(b.rain)
      || wth_f6(b.dewp)
      || wth_f6(b.wind)
      || lpad('-99', 6)             -- PAR: not in silver; DSSAT derives what it needs from SRAD
      || wth_f6(b.rhum)
  FROM wth_body(p_lat, p_lon, p_year, p_rain_source, p_chirps_mode) b
  ORDER BY b.date
$$;

CREATE OR REPLACE FUNCTION wth_file(
    p_lat DOUBLE PRECISION,
    p_lon DOUBLE PRECISION,
    p_year INT,
    p_rain_source TEXT DEFAULT 'era5',
    p_chirps_mode TEXT DEFAULT 'point',
    p_clim_from INT DEFAULT 1991,
    p_clim_to   INT DEFAULT 2020
)
RETURNS SETOF TEXT
LANGUAGE sql STABLE AS $$
  SELECT wth_header_lines(p_lat, p_lon, p_year, p_rain_source, p_clim_from, p_clim_to)
  UNION ALL
  SELECT wth_body_header_line()
  UNION ALL
  SELECT wth_body_lines(p_lat, p_lon, p_year, p_rain_source, p_chirps_mode)
$$;

-- The suggested filename for a given cell-year: <child_id><year>.WTH
CREATE OR REPLACE FUNCTION wth_filename(p_lat DOUBLE PRECISION, p_lon DOUBLE PRECISION, p_year INT)
RETURNS TEXT LANGUAGE sql IMMUTABLE AS $$
  SELECT era5_child_id(p_lat, p_lon) || p_year::text || '.WTH'
$$;


-- ---------------------------------------------------------------------------
-- 9. wth_qa — run this BEFORE writing files
--    A .WTH with silent gaps produces a simulation that runs and is wrong.
--
--    imputed is a BITMASK, not a boolean, so it says WHICH variable was repaired:
--      tmax=1  tmin=2  precip=4  srad=8  wind=16  tdew=32  rh=64  et0=128
--    Test one with `imputed & 4 <> 0` (precip repaired). Detail per cell-day is in
--    wth_imputation_log; whole-field defects are registered in wth_data_issues.
--
--    days_present < days_expected with rows in wth_qa_failures = the gap is explained:
--    the QA node quarantined those cell-days rather than loading bad values.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION wth_qa(p_lat DOUBLE PRECISION, p_lon DOUBLE PRECISION, p_year INT)
RETURNS TABLE (
    child_id      CHAR(4),
    parent_id     CHAR(4),
    days_present  BIGINT,
    days_expected INT,
    null_temp     BIGINT,
    null_srad     BIGINT,
    null_rain     BIGINT,
    era5t_rows    BIGINT,   -- preliminary ERA5T; the update DAG replaces these
    repaired_rows BIGINT,   -- imputed <> 0
    quarantined   BIGINT,   -- rows sitting in wth_qa_failures for this cell-year
    impossible    BIGINT    -- physically impossible rows; must be 0
)
LANGUAGE plpgsql STABLE AS $$
DECLARE
  v_child  CHAR(4) := era5_child_id(p_lat, p_lon);
  v_parent CHAR(4) := era5_parent_id(p_lat, p_lon, 4);
  v_d0     DATE    := make_date(p_year, 1, 1);
  v_d1     DATE    := make_date(p_year, 12, 31);
BEGIN
  RETURN QUERY
    SELECT v_child, v_parent,
           count(*),
           (v_d1 - v_d0 + 1),
           count(*) FILTER (WHERE w.tmax IS NULL OR w.tmin IS NULL),
           count(*) FILTER (WHERE w.srad IS NULL),
           count(*) FILTER (WHERE w.precip IS NULL),
           count(*) FILTER (WHERE w.is_preliminary),
           count(*) FILTER (WHERE w.imputed <> 0),
           (SELECT count(*) FROM wth_qa_failures q
             WHERE q.parent_id = v_parent AND q.child_id = v_child
               AND q.date BETWEEN v_d0 AND v_d1),
           count(*) FILTER (WHERE w.tmax < w.tmin OR w.precip < 0
                               OR w.rh < 0 OR w.rh > 100 OR w.srad < 0)
    FROM wth_base w
    WHERE w.parent_id = v_parent AND w.child_id = v_child
      AND w.date BETWEEN v_d0 AND v_d1;
END;
$$;

-- Exactly which days are absent from wth_base for this cell-year.
CREATE OR REPLACE FUNCTION wth_missing_days(p_lat DOUBLE PRECISION, p_lon DOUBLE PRECISION, p_year INT)
RETURNS TABLE (missing_date DATE)
LANGUAGE plpgsql STABLE AS $$
DECLARE
  v_child  CHAR(4) := era5_child_id(p_lat, p_lon);
  v_parent CHAR(4) := era5_parent_id(p_lat, p_lon, 4);
BEGIN
  RETURN QUERY
    SELECT d::date
    FROM generate_series(make_date(p_year,1,1), make_date(p_year,12,31), interval '1 day') d
    WHERE NOT EXISTS (
      SELECT 1 FROM wth_base w
      WHERE w.parent_id = v_parent AND w.child_id = v_child AND w.date = d::date)
    ORDER BY 1;
END;
$$;


-- ===========================================================================
-- 10. Verify the install. Every *_bad column below must be 0 — these functions
--     must reproduce the codes already stored in the grid tables, because the
--     database was built with the Python encoders in src/grid/.
-- ===========================================================================

SELECT count(*) AS checked,
       count(*) FILTER (WHERE era5_child_id(lat, lon)     <> child_id)  AS child_bad,
       count(*) FILTER (WHERE era5_parent_id(lat, lon, 4) <> parent_id) AS parent_bad,
       count(*) FILTER (WHERE era5_parent_of(child_id)    <> parent_id) AS parent_of_bad
FROM (SELECT * FROM era5_land_base_grid TABLESAMPLE SYSTEM (1)) s;

-- Skip this one if chirps_base_grid is empty (the fine grid is region-scoped —
-- only built extents exist).
SELECT count(*) AS checked,
       count(*) FILTER (WHERE chirps_fine_id(lat, lon)    <> fine_id)    AS fine_bad,
       count(*) FILTER (WHERE chirps_fparent_id(lat, lon) <> fparent_id) AS fparent_bad,
       count(*) FILTER (WHERE chirps_fparent_of(fine_id)  <> fparent_id) AS fparent_of_bad
FROM chirps_base_grid;


-- ===========================================================================
-- 11. Usage — edit the coordinates and run. (Tocantins/Para, Brazil.)
-- ===========================================================================

-- Where does this coordinate land?
--   SELECT era5_child_id(-5.175, -50.725) AS child_id,
--          era5_parent_id(-5.175, -50.725, 4) AS parent_id,
--          chirps_fine_id(-5.175, -50.725) AS fine_id,
--          wth_filename(-5.175, -50.725, 2020) AS filename;

-- Header only:
--   SELECT * FROM wth_header(-5.175, -50.725);
--   SELECT * FROM wth_header_lines(-5.175, -50.725, 2020);

-- Body only, ERA5 rain:
--   SELECT * FROM wth_body(-5.175, -50.725, 2020) LIMIT 10;
--   SELECT * FROM wth_body_lines(-5.175, -50.725, 2020) LIMIT 10;

-- Body only, CHIRPS v2 rain (nearest 0.05 deg cell):
--   SELECT * FROM wth_body(-5.175, -50.725, 2020, 'chirps_v2') LIMIT 10;

-- Body only, CHIRPS v3 reanalysis, area-weighted over the whole ERA5 cell:
--   SELECT * FROM wth_body(-5.175, -50.725, 2020, 'chirps_v3_rnl', 'weighted') LIMIT 10;

-- The complete file:
--   SELECT * FROM wth_file(-5.175, -50.725, 2020);
--   SELECT * FROM wth_file(-5.175, -50.725, 2020, 'chirps_v2');

-- Preflight QA:
--   SELECT * FROM wth_qa(-5.175, -50.725, 2020);
--   SELECT * FROM wth_missing_days(-5.175, -50.725, 2020);

-- Compare rain sources side by side before choosing one:
--   SELECT e.date, e.rain AS era5, c2.rain AS chirps_v2, c3.rain AS chirps_v3_rnl
--   FROM wth_body(-5.175, -50.725, 2020) e
--   JOIN wth_body(-5.175, -50.725, 2020, 'chirps_v2')     c2 ON c2.date = e.date
--   JOIN wth_body(-5.175, -50.725, 2020, 'chirps_v3_rnl') c3 ON c3.date = e.date
--   ORDER BY e.date;

-- Every cell inside a polygon, one row per station (feed these to scripts/wth_export.py):
--   SELECT g.child_id, g.parent_id, g.lat, g.lon
--   FROM era5_land_base_grid g
--   WHERE ST_Intersects(g.geom, ST_GeomFromText(
--           'POLYGON((-51.0 -5.5, -50.2 -5.5, -50.2 -4.8, -51.0 -4.8, -51.0 -5.5))', 4326))
--     AND g.is_land
--   ORDER BY g.child_id;

-- Writing a file from psql (DBeaver: right-click the grid > Export resultset > TXT,
-- no header, no delimiters):
--   docker compose exec -T postgres psql -U era5 -d era5 -qtA \
--       -c "SELECT * FROM wth_file(-5.175, -50.725, 2020)" > BSAD2020.WTH


-- ===========================================================================
-- 12. Teardown (uncomment to remove everything this file created)
-- ===========================================================================
-- DROP FUNCTION IF EXISTS wth_file(double precision, double precision, int, text, text, int, int);
-- DROP FUNCTION IF EXISTS wth_header_lines(double precision, double precision, int, text, int, int);
-- DROP FUNCTION IF EXISTS wth_body_lines(double precision, double precision, int, text, text);
-- DROP FUNCTION IF EXISTS wth_body(double precision, double precision, int, text, text);
-- DROP FUNCTION IF EXISTS wth_header(double precision, double precision, int, int);
-- DROP FUNCTION IF EXISTS wth_qa(double precision, double precision, int);
-- DROP FUNCTION IF EXISTS wth_missing_days(double precision, double precision, int);
-- DROP FUNCTION IF EXISTS wth_filename(double precision, double precision, int);
-- DROP FUNCTION IF EXISTS wth_rain_source_code(text);
-- DROP FUNCTION IF EXISTS wth_f6(double precision), wth_wind_kmday(double precision),
--                         wth_body_header_line(), wth_station_header_line();
-- DROP FUNCTION IF EXISTS era5_child_id(double precision, double precision),
--                         era5_parent_id(double precision, double precision, int),
--                         era5_parent_of(text, int),
--                         era5_cell_center(double precision, double precision),
--                         era5_lat_idx(double precision), era5_lon_idx(double precision);
-- DROP FUNCTION IF EXISTS chirps_fine_id(double precision, double precision),
--                         chirps_fparent_id(double precision, double precision),
--                         chirps_fparent_of(text),
--                         chirps_lat_idx(double precision), chirps_lon_idx(double precision);
-- DROP FUNCTION IF EXISTS base36(bigint, int), base36_decode(text), round_half_even(numeric);
