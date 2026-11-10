-- ===========================================================================
-- soil_helpers.sql — look up a DSSAT soil profile from a pair of coordinates.
--
-- The SQL mirror of src/db/soil_query.py, for anyone working in DBeaver rather than
-- Python. Companion to docs/step6_soil_intake.md.
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
--       -f - < sql/soil_helpers.sql
--
-- Read-only: functions only, no tables, no data touched. Safe on a live database and
-- safe to re-run. Requires soil_profile_points (the soil_grid_build DAG).
--
-- Every parameter is prefixed p_. That is not style, it is correctness: in a SQL-language
-- function a bare `lat` inside a query over soil_profile_points resolves to the TABLE's
-- lat column, not to the parameter, so `WHERE s.cell5m = soil_cell5m(lat, lon)` silently
-- becomes `s.cell5m = s.cell5m` — true for every row, and the function returns whichever
-- row it scanned first. Caught in review; do not un-prefix these.
-- ===========================================================================

-- Dropped rather than replaced: CREATE OR REPLACE refuses to rename input parameters, so
-- a database holding the pre-p_ version needs these gone first.
DROP FUNCTION IF EXISTS soil_id_at(DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION);
DROP FUNCTION IF EXISTS soil_profile_near(DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION);
DROP FUNCTION IF EXISTS soil_profile_at(DOUBLE PRECISION, DOUBLE PRECISION);
DROP FUNCTION IF EXISTS soil_cell5m_center(INTEGER);
DROP FUNCTION IF EXISTS soil_cell5m(DOUBLE PRECISION, DOUBLE PRECISION);


-- ---------------------------------------------------------------------------
-- 1. coordinate -> cell5m
--    The soil points sit on a regular 5 arc-min grid: 12 cells per degree, 4320
--    columns, row 0 at the north pole, row-major, 0-indexed. So the containing cell
--    is arithmetic and the lookup below is a primary-key hit — no spatial index.
--
--    This is a CONTAINING-cell rule (floor), not nearest-centre (round): a coordinate
--    belongs to the cell whose box it falls in, which is what the source ids mean.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION soil_cell5m(p_lat DOUBLE PRECISION, p_lon DOUBLE PRECISION)
RETURNS INTEGER
LANGUAGE sql IMMUTABLE STRICT AS $$
  SELECT (LEAST(GREATEST(floor((90.0 - p_lat) * 12)::int, 0), 2159) * 4320)
       + (floor(((p_lon + 180.0)::numeric % 360.0) * 12)::int % 4320);
$$;

COMMENT ON FUNCTION soil_cell5m(DOUBLE PRECISION, DOUBLE PRECISION) IS
  'HarvestChoice 5 arc-min cell id containing (lat, lon). Primary key of soil_profile_points.';


-- ---------------------------------------------------------------------------
-- 2. cell5m -> its centre, for reporting what was actually returned
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION soil_cell5m_center(p_cell5m INTEGER)
RETURNS TABLE (lat DOUBLE PRECISION, lon DOUBLE PRECISION)
LANGUAGE sql IMMUTABLE STRICT AS $$
  SELECT 90.0 - ((p_cell5m / 4320) + 0.5) / 12.0,
        -180.0 + ((p_cell5m % 4320) + 0.5) / 12.0;
$$;


-- ---------------------------------------------------------------------------
-- 3. coordinate -> soil profile, exact cell only
--    Returns zero rows when the coordinate lands on a cell with no profile — the
--    layer is land-only, so coastal, lake and ice coordinates legitimately miss.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION soil_profile_at(p_lat DOUBLE PRECISION, p_lon DOUBLE PRECISION)
RETURNS TABLE (
  cell5m    INTEGER,
  soil_id   VARCHAR,
  iso2      VARCHAR,
  cell_lat  DOUBLE PRECISION,
  cell_lon  DOUBLE PRECISION,
  child_id  CHAR(4),
  parent_id CHAR(4),
  dist_km   DOUBLE PRECISION
)
LANGUAGE sql STABLE STRICT AS $$
  SELECT s.cell5m, s.soil_id, s.iso2, s.lat, s.lon, s.child_id, s.parent_id, 0.0
  FROM soil_profile_points s
  WHERE s.cell5m = soil_cell5m(p_lat, p_lon);
$$;


-- ---------------------------------------------------------------------------
-- 4. coordinate -> nearest soil profile within a radius
--    The one place soil_profile_points.geom and its GiST index earn their keep.
--    The bounding box is widened in x by 1/cos(lat): longitude degrees shrink toward
--    the poles, and a square box in degrees would under-reach and report "no profile"
--    where one sits well inside the radius. Distance itself is measured on the
--    geography, in true metres.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION soil_profile_near(
  p_lat DOUBLE PRECISION, p_lon DOUBLE PRECISION, p_max_km DOUBLE PRECISION DEFAULT 25.0
)
RETURNS TABLE (
  cell5m    INTEGER,
  soil_id   VARCHAR,
  iso2      VARCHAR,
  cell_lat  DOUBLE PRECISION,
  cell_lon  DOUBLE PRECISION,
  child_id  CHAR(4),
  parent_id CHAR(4),
  dist_km   DOUBLE PRECISION
)
LANGUAGE sql STABLE STRICT AS $$
  WITH p AS (SELECT ST_SetSRID(ST_MakePoint(p_lon, p_lat), 4326) AS pt,
                    p_max_km / 111.32                            AS dy,
                    (p_max_km / 111.32) / GREATEST(cos(radians(p_lat)), 1e-3) AS dx)
  SELECT s.cell5m, s.soil_id, s.iso2, s.lat, s.lon, s.child_id, s.parent_id,
         ST_Distance(s.geom::geography, p.pt::geography) / 1000.0
  FROM soil_profile_points s, p
  WHERE s.geom && ST_Expand(p.pt, p.dx, p.dy)
    AND ST_DWithin(s.geom::geography, p.pt::geography, p_max_km * 1000.0)
  ORDER BY s.geom <-> p.pt
  LIMIT 1;
$$;


-- ---------------------------------------------------------------------------
-- 5. coordinate -> soil profile, exact then nearest
--    What most callers want: the profile in the cell, or the closest one if that cell
--    is empty. dist_km is 0 for an exact hit, so the caller can tell them apart.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION soil_id_at(
  p_lat DOUBLE PRECISION, p_lon DOUBLE PRECISION, p_max_km DOUBLE PRECISION DEFAULT 25.0
)
RETURNS TABLE (
  cell5m    INTEGER,
  soil_id   VARCHAR,
  iso2      VARCHAR,
  cell_lat  DOUBLE PRECISION,
  cell_lon  DOUBLE PRECISION,
  child_id  CHAR(4),
  parent_id CHAR(4),
  dist_km   DOUBLE PRECISION
)
LANGUAGE sql STABLE STRICT AS $$
  SELECT * FROM soil_profile_at(p_lat, p_lon)
  UNION ALL
  SELECT * FROM soil_profile_near(p_lat, p_lon, p_max_km)
  WHERE NOT EXISTS (SELECT 1 FROM soil_profile_at(p_lat, p_lon))
  LIMIT 1;
$$;


-- ===========================================================================
-- VERIFY (expect: the encoding round-trips, and a known point resolves)
-- ===========================================================================

-- Every stored point must map back to its own id from its own coordinates.
-- SELECT count(*) AS mismatches
-- FROM soil_profile_points
-- WHERE cell5m <> soil_cell5m(lat, lon);          -- expect 0

-- Andorra, the first record in the source file.
-- SELECT * FROM soil_id_at(42.625, 1.542);        -- AD02455938, dist_km 0

-- Montevideo, Uruguay.
-- SELECT * FROM soil_id_at(-34.9, -56.2);

-- Weather and soil for one site, in one query.
-- SELECT s.soil_id, s.child_id, g.lat, g.lon, g.elevation, g.t_zone
-- FROM soil_id_at(-34.9, -56.2) s
-- JOIN era5_land_base_grid g USING (child_id);


-- ===========================================================================
-- TEARDOWN (uncomment to remove everything this file created)
-- ===========================================================================
-- DROP FUNCTION IF EXISTS soil_id_at(DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION);
-- DROP FUNCTION IF EXISTS soil_profile_near(DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION);
-- DROP FUNCTION IF EXISTS soil_profile_at(DOUBLE PRECISION, DOUBLE PRECISION);
-- DROP FUNCTION IF EXISTS soil_cell5m_center(INTEGER);
-- DROP FUNCTION IF EXISTS soil_cell5m(DOUBLE PRECISION, DOUBLE PRECISION);
