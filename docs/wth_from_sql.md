# Building DSSAT `.WTH` files with SQL

A teaching cookbook. Every recipe is a query you run against the silver database — no Python,
no pipeline, no Airflow. You point at a coordinate or a polygon, pick a precipitation source
(ERA5, CHIRPS v2, CHIRPS v3), and get a `.WTH` file out the other end.

> **Status.** Gold (`.WTH` materialization) is *deferred* in code — PLANNING.md §9 specifies the
> conversions but no module writes files yet, and the header fields `TAV`/`AMP`/`ELEV` are
> explicitly listed as pending. This document is the interim path: it does the whole job in SQL,
> and doubles as the spec anyone implementing the gold module should read first.

Every query here was run against the live database while this document was written. Where a
number appears (row counts, timings) it is measured, not estimated.

**If you just want the files, skip to the two runnable artifacts.** This document explains *why*
each query is shaped the way it is; these two do the work:

| File | What it is |
|---|---|
| `sql/wth_helpers.sql` | Import once into DBeaver/psql. Installs the encoders plus `wth_header()`, `wth_body()`, `wth_file()`, `wth_qa()`. Then `SELECT * FROM wth_file(-5.175, -50.725, 2020, 'chirps_v2');` is the whole job. Functions only — no tables, no data touched, safe to re-run. |
| `scripts/wth_export.py` | psycopg2 client over those same functions. `uv run python scripts/wth_export.py file -5.175 -50.725 2020 --rain chirps_v2 --out ./wth`. Also `header`, `body`, `qa`, `compare`, `polygon`. |

The two agree by construction — the Python module calls the SQL functions rather than
re-implementing the formulas, and their output is byte-identical to Recipe 2 below. Read on if
you want to know what those functions are doing.

---

## 0. What you are working with

| Table | Grid | One row is | Scale (this database) |
|---|---|---|---|
| `era5_land_base_grid` | 0.25° ERA5 | a cell: `lat`, `lon`, `elevation`, `t_zone`, `geom` | 1,038,240 global (shipped seed) |
| `wth_base` | 0.25° ERA5 | a cell-day, all variables wide | 173,307,088 rows, 10,183 cells, 1952-01-01 → 2026-08-05 |
| `chirps_base_grid` | 0.05° CHIRPS | a fine cell: `lat`, `lon`, `geom` | region-scoped — only built extents exist |
| `wth_precip_alt` | 0.05° CHIRPS | a fine-cell-day-source precipitation | 560,557,878 rows, 1981-01-01 → 2026-06-30 |
| `wth_precip_alt_source` | — | a CHIRPS product (`source` code ↔ name) | 2 = `chirps_v2`, 3 = `chirps_v3_rnl` |
| `chirps_era5_map` | both | area weight of a fine cell inside an ERA5 cell | built per-extent; **empty until you build it** |

Silver units — these are already what DSSAT wants, except wind:

| Column | Unit | `.WTH` field | Conversion |
|---|---|---|---|
| `tmax`, `tmin`, `tdew` | °C | `TMAX`, `TMIN`, `DEWP` | none |
| `precip` | mm/day | `RAIN` | none |
| `srad` | MJ/m²/day | `SRAD` | none |
| `rh` | % | `RHUM` | none |
| `wind` | m/s **at 10 m** | `WIND` | `× 0.748 × 86.4` = `× 64.6272` → km/day at 2 m |
| `et0` | mm/day | — | not a `.WTH` field; useful for sanity checks |

**The one physical subtlety.** `wth_base.date` is a **local** calendar day — the 24-hour window
was shifted per cell by `era5_land_base_grid.t_zone` (standard UTC offset, minutes) at reduction
time. `wth_precip_alt.date` is the **CHIRPS product day**, a fixed UTC-anchored window. They are
different 24-hour windows, and no amount of SQL fixes that after the fact: daily reduction is
lossy. Mixing them (Recipe 4) is legitimate and common, but the mismatch is real and you should
say so in the file's description line. At monthly or seasonal aggregation it is immaterial; on a
single day it is not.

---

## 1. Connecting

From the repo root, with the stack up (`docker compose up -d`):

```bash
# interactive
docker compose exec postgres psql -U era5 -d era5

# run a script and capture raw rows (no headers, no padding, quiet) — this is the mode
# every file-producing recipe below uses
docker compose exec -T postgres psql -U era5 -d era5 -qtA -f - < myquery.sql > OUT.WTH
```

`-q` quiet, `-t` tuples only (no header/footer), `-A` unaligned (no column padding). Miss any of
the three and you get decoration in the middle of your weather file.

DBeaver or any client works equally well for exploring; the `-qtA` form is only needed when the
query output *is* the file.

---

## 2. Anatomy of a `.WTH` file

```
*WEATHER DATA : BSAD ERA5 0.25deg 2020

@ INSI      LAT     LONG  ELEV   TAV   AMP REFHT WNDHT
  BSAD   -5.250  -50.750   223  26.7   1.3   2.0   2.0
@DATE  SRAD  TMAX  TMIN  RAIN  DEWP  WIND   PAR  RHUM
20001  13.1  28.8  22.1  12.4  21.9 108.0   -99  80.9
20002  10.9  29.7  22.9   5.3  23.0  80.2   -99  81.9
```

Rules that matter:

- **Filename `XXXXYYYY.WTH`** — 4-char station code + 4-digit year. The station code *is* the
  `child_id`: base-36, reversible, so a file decodes straight back to a coordinate with no lookup
  table (PLANNING.md §6.2). That is the whole point of the encoding.
- **Fixed columns.** `@DATE` is 5 chars; every field after it is 6 chars, right-aligned. The
  header block is `@ INSI` (6) + `LAT` (9) + `LONG` (9) + `ELEV`/`TAV`/`AMP`/`REFHT`/`WNDHT`
  (6 each). Several DSSAT modules read by column position, so match the widths.
- **`@DATE` is `YYDDD`** — 2-digit year + day of year. Two-digit years are ambiguous across the
  century; one file per year with the year in the *filename* is what resolves it. (Some DSSAT
  4.7+ tools accept a 7-char `YYYYDDD`. Check your target version before relying on it.)
- **Missing is `-99`**, never blank and never `NULL`. Every recipe below wraps values in
  `COALESCE(x, -99)`.
- **`PAR` is not in silver.** Emitted as `-99`. DSSAT computes what it needs from `SRAD`.
- **`WNDHT` is 2.0, not 10.0** — because the `× 0.748` factor in the wind conversion *is* the
  10 m → 2 m FAO-56 adjustment. Writing `10.0` there while converting to 2 m double-counts the
  correction. This is the single easiest mistake to make in the header.
- **`REFHT` is 2.0** — the temperature/humidity reference height.

⚠ **Station-code charset.** Base-36 codes may lead with a digit (`0QML`, `5L7NT`). PLANNING.md
§17 flags this as unconfirmed against DSSAT's station-code conventions. If your DSSAT build
rejects a leading digit, prefix a letter and keep the mapping — do **not** renumber the grid.

---

## 3. Install the helper functions (once)

These reproduce `src/grid/encoding.py` and `src/grid/fine_encoding.py` in pure SQL, so a
coordinate resolves to its cell arithmetically — no PostGIS, no spatial index, no Python.

Paste this block once into your database. They are read-only helpers — they create no tables and
touch no data — so installing them is safe on a live database. To remove them:
`DROP FUNCTION IF EXISTS base36, base36_decode, round_half_even, era5_lat_idx, era5_lon_idx,
era5_child_id, era5_parent_id, era5_parent_of, chirps_lat_idx, chirps_lon_idx, chirps_fine_id,
chirps_fparent_id, chirps_fparent_of;`

```sql
-- ---------------------------------------------------------------------------
-- base-36 codec (the alphabet is the canonical one from src/grid/spec.py)
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
    IF pos = 0 THEN RAISE EXCEPTION 'bad base-36 char in %', code; END IF;
    n := n * 36 + (pos - 1);
  END LOOP;
  RETURN n;
END;
$$;

-- Python's round() is banker's rounding (ties to even); Postgres round() is half-away-from-zero.
-- They differ only for a coordinate landing exactly on a 0.25 deg cell boundary (x.125) — but
-- when they differ they pick a different cell, and the database was built with the Python rule.
CREATE OR REPLACE FUNCTION round_half_even(x NUMERIC) RETURNS BIGINT
LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE
    WHEN x - floor(x) = 0.5 THEN
      CASE WHEN floor(x)::BIGINT % 2 = 0 THEN floor(x)::BIGINT ELSE floor(x)::BIGINT + 1 END
    ELSE round(x)::BIGINT
  END
$$;

-- ---------------------------------------------------------------------------
-- 0.25 deg ERA5 grid — cell CENTRES sit on multiples of 0.25, so bin with round()
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION era5_lat_idx(lat DOUBLE PRECISION) RETURNS BIGINT
LANGUAGE sql IMMUTABLE AS $$ SELECT round_half_even((90.0 - lat::numeric) / 0.25) $$;

CREATE OR REPLACE FUNCTION era5_lon_idx(lon DOUBLE PRECISION) RETURNS BIGINT
LANGUAGE sql IMMUTABLE AS $$ SELECT round_half_even(mod(mod(lon::numeric, 360) + 360, 360) / 0.25) $$;

CREATE OR REPLACE FUNCTION era5_child_id(lat DOUBLE PRECISION, lon DOUBLE PRECISION) RETURNS CHAR(4)
LANGUAGE sql IMMUTABLE AS $$
  SELECT base36(era5_lat_idx(lat) * 1440 + era5_lon_idx(lon), 4)::CHAR(4)
$$;

CREATE OR REPLACE FUNCTION era5_parent_id(lat DOUBLE PRECISION, lon DOUBLE PRECISION, b INT DEFAULT 4)
RETURNS CHAR(4) LANGUAGE sql IMMUTABLE AS $$
  SELECT base36((era5_lat_idx(lat) / b) * ((1440 + b - 1) / b) + (era5_lon_idx(lon) / b), 4)::CHAR(4)
$$;

-- parent straight from a child code — no coordinates needed. This is what lets a region query
-- put the partition key in the WHERE clause without a second round-trip to the grid.
CREATE OR REPLACE FUNCTION era5_parent_of(child_id TEXT, b INT DEFAULT 4) RETURNS CHAR(4)
LANGUAGE sql IMMUTABLE AS $$
  SELECT base36(((base36_decode(rtrim(child_id)) / 1440) / b) * ((1440 + b - 1) / b)
                + ((base36_decode(rtrim(child_id)) % 1440) / b), 4)::CHAR(4)
$$;

-- ---------------------------------------------------------------------------
-- 0.05 deg CHIRPS fine grid — cell EDGES sit on multiples of 0.05, so bin with floor().
-- Multiply by 20 rather than divide by 0.05: 0.05 has no exact binary form and dividing puts
-- edge-aligned coordinates on the wrong side of the floor about half the time.
-- Codes are 5 chars wide; a 4-char code is ALWAYS the 0.25 deg grid.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION chirps_lat_idx(lat DOUBLE PRECISION) RETURNS BIGINT
LANGUAGE sql IMMUTABLE AS $$
  SELECT least(greatest(floor((60.0 - lat) * 20 + 1e-9)::BIGINT, 0), 2399)
$$;

CREATE OR REPLACE FUNCTION chirps_lon_idx(lon DOUBLE PRECISION) RETURNS BIGINT
LANGUAGE sql IMMUTABLE AS $$
  SELECT least(greatest(floor(mod(mod(lon::numeric, 360) + 360, 360) * 20 + 1e-9)::BIGINT, 0), 7199)
$$;

CREATE OR REPLACE FUNCTION chirps_fine_id(lat DOUBLE PRECISION, lon DOUBLE PRECISION) RETURNS CHAR(5)
LANGUAGE sql IMMUTABLE AS $$
  SELECT base36(chirps_lat_idx(lat) * 7200 + chirps_lon_idx(lon), 5)::CHAR(5)
$$;

CREATE OR REPLACE FUNCTION chirps_fparent_id(lat DOUBLE PRECISION, lon DOUBLE PRECISION) RETURNS CHAR(5)
LANGUAGE sql IMMUTABLE AS $$
  SELECT base36((chirps_lat_idx(lat) / 20) * 360 + (chirps_lon_idx(lon) / 20), 5)::CHAR(5)
$$;

CREATE OR REPLACE FUNCTION chirps_fparent_of(fine_id TEXT) RETURNS CHAR(5)
LANGUAGE sql IMMUTABLE AS $$
  SELECT base36(((base36_decode(rtrim(fine_id)) / 7200) / 20) * 360
                + ((base36_decode(rtrim(fine_id)) % 7200) / 20), 5)::CHAR(5)
$$;
```

**Verify them before trusting them.** The grid tables are the ground truth — the functions must
reproduce the codes already stored:

```sql
SELECT count(*) AS checked,
       count(*) FILTER (WHERE era5_child_id(lat, lon)     <> child_id)  AS child_bad,
       count(*) FILTER (WHERE era5_parent_id(lat, lon, 4) <> parent_id) AS parent_bad,
       count(*) FILTER (WHERE era5_parent_of(child_id)    <> parent_id) AS parent_of_bad
FROM (SELECT * FROM era5_land_base_grid TABLESAMPLE SYSTEM (1)) s;

SELECT count(*) AS checked,
       count(*) FILTER (WHERE chirps_fine_id(lat, lon)    <> fine_id)    AS fine_bad,
       count(*) FILTER (WHERE chirps_fparent_id(lat, lon) <> fparent_id) AS fparent_bad,
       count(*) FILTER (WHERE chirps_fparent_of(fine_id)  <> fparent_id) AS fparent_of_bad
FROM chirps_base_grid;
```

All `*_bad` columns must be `0`. (Measured here: a ~1% sample of the ERA5 grid — around 10,000
cells per run — and all 16,867 fine cells, zero mismatches. The functions were also checked
against the Python encoders over 204 coordinates, including boundary ties; see gotcha 2.)

⚠ **`b = 4` is immutable.** The `b` in `era5_parent_id` is the parent block size the database was
built with. It is baked into every stored `parent_id` and every partition name. Passing a
different `b` silently produces codes that match nothing.

---

## 4. Recipe 1 — coordinates → cell

Two ways, and the difference matters.

**Arithmetic (fast, no index, the default):**

```sql
SELECT era5_child_id(-5.175, -50.725)    AS child_id,
       era5_parent_id(-5.175, -50.725, 4) AS parent_id;
--  child_id | parent_id
--  BSAD     | 0QML
```

**Grid lookup (also gives you elevation, timezone, land flag):**

```sql
SELECT child_id, parent_id, lat, lon, elevation, t_zone, is_land
FROM era5_land_base_grid
WHERE child_id = era5_child_id(-5.175, -50.725);
--  BSAD | 0QML | -5.25 | -50.75 | 223.16275 | -180 | t
```

Note the returned `lat`/`lon` are the **cell centre**, not your input. Report the centre in the
`.WTH` header — that is where the data actually is. `t_zone = -180` means this cell's daily
window is UTC−3.

The fine (CHIRPS) grid works the same way:

```sql
SELECT chirps_fine_id(-5.175, -50.725)    AS fine_id,
       chirps_fparent_id(-5.175, -50.725) AS fparent_id;
--  5L7NT | 00IAL
```

> **Always put `parent_id` in the `WHERE` clause.** `wth_base` is `PARTITION BY LIST (parent_id)`.
> With the literal present, Postgres prunes to one partition at plan time. Without it, it
> considers all of them. Same for `fparent_id` on `wth_precip_alt`. Section 9 shows what this
> costs when you get it wrong.

---

## 5. Recipe 2 — one cell, one year, pure ERA5 → a complete `.WTH`

Save as `wth_one.sql`. The `\set` lines at the top are the only things you edit.

```sql
\set lat -5.175
\set lon -50.725
\set yr  2020
\set clim_from 1991
\set clim_to   2020

-- Resolve the codes into psql variables FIRST. This keeps them as literals in the query below,
-- which is what lets partition pruning happen at plan time (see section 9).
SELECT era5_child_id(:lat, :lon)     AS child,
       era5_parent_id(:lat, :lon, 4) AS parent \gset

WITH cell AS (
    SELECT g.child_id, g.parent_id, g.lat, g.lon, g.elevation
    FROM era5_land_base_grid g
    WHERE g.child_id = :'child'
),
monthly AS (                                    -- climatology for TAV / AMP
    SELECT date_part('month', w.date) AS mon,
           avg((w.tmax + w.tmin) / 2.0)  AS tmean
    FROM wth_base w
    WHERE w.parent_id = :'parent' AND w.child_id = :'child'
      AND w.date >= make_date(:clim_from, 1, 1)
      AND w.date <= make_date(:clim_to, 12, 31)
    GROUP BY 1
),
clim AS (
    SELECT avg(tmean)                       AS tav,
           (max(tmean) - min(tmean)) / 2.0   AS amp
    FROM monthly
),
body AS (
    SELECT w.date,
           to_char(w.date, 'YYDDD')
        || lpad(to_char(COALESCE(w.srad,   -99)::numeric, 'FM99990.0'), 6)   -- SRAD MJ/m2/day
        || lpad(to_char(COALESCE(w.tmax,   -99)::numeric, 'FM99990.0'), 6)   -- TMAX degC
        || lpad(to_char(COALESCE(w.tmin,   -99)::numeric, 'FM99990.0'), 6)   -- TMIN degC
        || lpad(to_char(COALESCE(w.precip, -99)::numeric, 'FM99990.0'), 6)   -- RAIN mm
        || lpad(to_char(COALESCE(w.tdew,   -99)::numeric, 'FM99990.0'), 6)   -- DEWP degC
        || lpad(to_char(COALESCE(w.wind * 64.6272, -99)::numeric, 'FM99990.0'), 6)  -- WIND km/day @2m
        || lpad('-99', 6)                                                    -- PAR (not in silver)
        || lpad(to_char(COALESCE(w.rh,     -99)::numeric, 'FM99990.0'), 6)   -- RHUM %
           AS line
    FROM wth_base w
    WHERE w.parent_id = :'parent' AND w.child_id = :'child'
      AND w.date >= make_date(:yr, 1, 1)
      AND w.date <= make_date(:yr, 12, 31)
)
SELECT line FROM (
    SELECT 0 AS ord, NULL::date AS d,
           '*WEATHER DATA : ' || c.child_id || ' ERA5 0.25deg local-day ' || :yr AS line
    FROM cell c
    UNION ALL SELECT 1, NULL, ''
    UNION ALL SELECT 2, NULL, '@ INSI      LAT     LONG  ELEV   TAV   AMP REFHT WNDHT'
    UNION ALL
    SELECT 3, NULL,
           '  ' || c.child_id
        || lpad(to_char(c.lat::numeric, 'FM9990.000'), 9)
        || lpad(to_char(c.lon::numeric, 'FM9990.000'), 9)
        || lpad(to_char(COALESCE(c.elevation, -99)::numeric, 'FM99990'), 6)
        || lpad(to_char(k.tav::numeric, 'FM9990.0'), 6)
        || lpad(to_char(k.amp::numeric, 'FM9990.0'), 6)
        || lpad('2.0', 6)      -- REFHT
        || lpad('2.0', 6)      -- WNDHT: 2 m, because WIND was already adjusted 10 m -> 2 m
    FROM cell c CROSS JOIN clim k
    UNION ALL SELECT 4, NULL, '@DATE  SRAD  TMAX  TMIN  RAIN  DEWP  WIND   PAR  RHUM'
    UNION ALL SELECT 5, b.date, b.line FROM body b
) s
ORDER BY ord, d;
```

Run it:

```bash
docker compose exec -T postgres psql -U era5 -d era5 -qtA -f - < wth_one.sql > BSAD2020.WTH
```

Result — 371 lines (5 header + 366 days of a leap year), 0.24 s:

```
*WEATHER DATA : BSAD ERA5 0.25deg local-day 2020

@ INSI      LAT     LONG  ELEV   TAV   AMP REFHT WNDHT
  BSAD   -5.250  -50.750   223  26.7   1.3   2.0   2.0
@DATE  SRAD  TMAX  TMIN  RAIN  DEWP  WIND   PAR  RHUM
20001  13.1  28.8  22.1  12.4  21.9 108.0   -99  80.9
20002  10.9  29.7  22.9   5.3  23.0  80.2   -99  81.9
20003   4.9  26.0  22.4  22.5  22.7  44.7   -99  91.5
```

**How the assembly works.** The `UNION ALL` block is the whole trick: header lines get `ord`
0–4, data rows get `ord` 5 and sort among themselves by date. One `ORDER BY ord, d` at the end
puts the file in order. Everything is text by that point, so `-qtA` dumps it verbatim.

**`FM99990.0`** — `FM` strips `to_char`'s leading space and padding; `99990.0` forces one decimal
and at least one integer digit (so `0.5` is not rendered as `.5`). The explicit `lpad(..., 6)`
then does the column alignment. Widen the `9`s if you have a value that can exceed 9999.

---

## 6. Recipe 3 — the header numbers, and what they mean

`ELEV`, `TAV`, `AMP` are the fields PLANNING.md §9 lists as deferred. They are computable from
silver, which is why Recipe 2 already fills them.

```sql
\set child 'BSAD'
\set parent '0QML'

-- ELEV: straight from the grid seed, derived from geopotential (z / 9.80665).
-- Use the 0.25 deg model orography, NOT a high-res DEM — ET0 in silver was computed against it,
-- and a mismatched elevation makes the two inconsistent.
SELECT elevation FROM era5_land_base_grid WHERE child_id = :'child';

-- TAV: long-term mean air temperature.
-- AMP: half the annual range of the monthly means.
WITH monthly AS (
    SELECT date_part('month', date) AS mon, avg((tmax + tmin) / 2.0) AS tmean
    FROM wth_base
    WHERE parent_id = :'parent' AND child_id = :'child'
      AND date BETWEEN '1991-01-01' AND '2020-12-31'
    GROUP BY 1
)
SELECT round(avg(tmean)::numeric, 1)                       AS tav,
       round(((max(tmean) - min(tmean)) / 2.0)::numeric, 1) AS amp
FROM monthly;
--  tav  | amp
--  26.7 | 1.3
```

Two judgement calls you are making here, whether you notice or not:

1. **`AMP` convention.** Written above as *half* the range of monthly means, which is what DSSAT's
   documentation describes as the annual amplitude. Some tooling in the wild uses the full range
   (`max − min`). Pick one, state it, and be consistent — a 2× error in `AMP` shifts soil
   temperature and therefore emergence and phenology.
2. **Climatology window.** 1991–2020 above (the current WMO normal). Using the simulated year's
   own 12 months instead makes `TAV`/`AMP` vary year to year, which is usually not what you want
   from a header meant to describe the site.

An `AMP` near 1.3 °C is not a bug — that is an equatorial site (5°S) with essentially no annual
temperature cycle. Check it against latitude before "fixing" it.

---

## 7. Recipe 4 — swapping precipitation for CHIRPS

CHIRPS lives on the 0.05° grid, 25× finer than ERA5, and in two versions. Temperature, radiation,
wind and humidity still come from ERA5; only `RAIN` changes.

> **Read the day-definition warning in section 0 before using these.** ERA5's day is the cell's
> local day; CHIRPS's is the product's own UTC-anchored day. The join on `date` treats them as
> the same window. They are not.

### 7a. Nearest fine cell (simplest, and what a point site usually wants)

One 0.05° cell — about 5.5 km — centred on the coordinate, not an average over 28 km.

```sql
\set lat -5.175
\set lon -50.725
\set yr  2020
\set src 'chirps_v2'          -- or 'chirps_v3_rnl'

SELECT era5_child_id(:lat, :lon)     AS child,
       era5_parent_id(:lat, :lon, 4) AS parent,
       chirps_fine_id(:lat, :lon)    AS fine,
       chirps_fparent_id(:lat, :lon) AS fparent,
       (SELECT source FROM wth_precip_alt_source WHERE code = :'src') AS srccode \gset

WITH rain AS (
    SELECT p.date, p.precip
    FROM wth_precip_alt p
    WHERE p.fparent_id = :'fparent' AND p.fine_id = :'fine' AND p.source = :srccode
      AND p.date >= make_date(:yr, 1, 1) AND p.date <= make_date(:yr, 12, 31)
)
SELECT to_char(w.date, 'YYDDD')
    || lpad(to_char(COALESCE(w.srad,   -99)::numeric, 'FM99990.0'), 6)
    || lpad(to_char(COALESCE(w.tmax,   -99)::numeric, 'FM99990.0'), 6)
    || lpad(to_char(COALESCE(w.tmin,   -99)::numeric, 'FM99990.0'), 6)
    || lpad(to_char(COALESCE(r.precip, -99)::numeric, 'FM99990.0'), 6)   -- RAIN from CHIRPS
    || lpad(to_char(COALESCE(w.tdew,   -99)::numeric, 'FM99990.0'), 6)
    || lpad(to_char(COALESCE(w.wind * 64.6272, -99)::numeric, 'FM99990.0'), 6)
    || lpad('-99', 6)
    || lpad(to_char(COALESCE(w.rh,     -99)::numeric, 'FM99990.0'), 6) AS line
FROM wth_base w
LEFT JOIN rain r ON r.date = w.date        -- LEFT: a CHIRPS gap becomes -99, not a dropped day
WHERE w.parent_id = :'parent' AND w.child_id = :'child'
  AND w.date >= make_date(:yr, 1, 1) AND w.date <= make_date(:yr, 12, 31)
ORDER BY w.date;
```

Drop this `body` into Recipe 2's `UNION ALL` scaffold and change the description line to name the
source — e.g. `'*WEATHER DATA : BSAD ERA5 T/SRAD/WIND + CHIRPS v2 RAIN (product day) 2020'`.

Side by side, the first days of 2020 at this cell:

```
    date    | era5_precip | chirps_precip
 2020-01-01 |   12.42     |          0.00
 2020-01-02 |    5.34     |          9.44
 2020-01-03 |   22.53     |         18.88
```

That is not a defect. It is the day-window offset plus two genuinely different estimates of
rainfall. Compare monthly totals, not individual days.

### 7b. Area-weighted to the whole ERA5 cell (the correct aggregate)

If your `.WTH` represents a 0.25° cell rather than a point, the honest CHIRPS number is the
area-weighted mean of the ~36 fine cells overlapping it. The two grids do **not** nest — ERA5 cell
edges fall on `0.125 + k×0.25`, which is not a multiple of 0.05 — so this is a stored spatial
join, never arithmetic.

Build the map once per extent (it is empty on a fresh database):

```bash
uv run python -m src.db.chirps_map          # builds chirps_era5_map + the precip_compare view
```

Then:

```sql
\set lat -5.175
\set lon -50.725
\set yr  2020
\set src 'chirps_v2'

SELECT era5_child_id(:lat, :lon)     AS child,
       era5_parent_id(:lat, :lon, 4) AS parent,
       (SELECT source FROM wth_precip_alt_source WHERE code = :'src') AS srccode \gset

-- Materialize the fine-cell list first: it gives the planner a concrete fparent_id set to prune
-- wth_precip_alt with, instead of computing the partition key inside the join.
CREATE TEMP TABLE fine_for_cell AS
SELECT m.fine_id, chirps_fparent_of(m.fine_id) AS fparent_id, m.weight
FROM chirps_era5_map m
WHERE m.child_id = :'child';

WITH rain AS (
    SELECT p.date,
           sum(p.precip * f.weight) / nullif(sum(f.weight), 0) AS precip,
           sum(f.weight)                                       AS coverage
    FROM fine_for_cell f
    JOIN wth_precip_alt p
      ON p.fparent_id = f.fparent_id AND p.fine_id = f.fine_id AND p.source = :srccode
    WHERE p.date >= make_date(:yr, 1, 1) AND p.date <= make_date(:yr, 12, 31)
    GROUP BY p.date
)
SELECT w.date, w.precip AS era5_rain, r.precip AS chirps_rain, r.coverage
FROM wth_base w
LEFT JOIN rain r ON r.date = w.date
WHERE w.parent_id = :'parent' AND w.child_id = :'child'
  AND w.date >= make_date(:yr, 1, 1) AND w.date <= make_date(:yr, 12, 31)
ORDER BY w.date;
```

`coverage` is the fraction of the ERA5 cell the fine grid actually covers. **`coverage < 1.0`
means you are averaging over part of the cell** — normal at the edge of a built extent, and
something to know before you read the number as a cell mean.

The `precip_compare` view (also created by `src.db.chirps_map`) wraps exactly this join for all
cells at once, with the caveat recorded in its `COMMENT ON VIEW`:

```sql
SELECT * FROM precip_compare
WHERE child_id = 'BSAD' AND date BETWEEN '2020-01-01' AND '2020-01-31';
```

### 7c. Which CHIRPS version?

```sql
SELECT source, code, collection, note FROM wth_precip_alt_source ORDER BY source;
```

| `source` | code | Use it when |
|---|---|---|
| 2 | `chirps_v2` | you want comparability with published crop-model work — this is the long-standing reference |
| 3 | `chirps_v3_rnl` | you want v3's improved totals, accepting that daily values are ERA5-disaggregated from pentad totals |
| 4 | `chirps_v3_sat` | you need low latency near the present (less stable history) |

For daily-resolution crop simulation, `chirps_v3_rnl`'s day-to-day structure is *derived*, not
observed — the pentad total is the quantity actually estimated. If daily rainfall sequencing
drives your result (it usually does for water-limited yield), prefer `chirps_v2` or ERA5 and say
which you used.

---

## 8. Recipe 5 — polygons

### 8a. List the cells in a polygon

The one place `geom` and its GiST index earn their keep.

```sql
WITH aoi AS (
    SELECT ST_GeomFromText(
      'POLYGON((-51.0 -5.5, -50.2 -5.5, -50.2 -4.8, -51.0 -4.8, -51.0 -5.5))', 4326) AS geom
)
SELECT g.child_id, g.parent_id, g.lat, g.lon, g.elevation,
       round((ST_Area(ST_Intersection(g.geom, a.geom)) / ST_Area(a.geom))::numeric, 4) AS area_weight
FROM era5_land_base_grid g
JOIN aoi a ON g.geom && a.geom
WHERE ST_Intersects(g.geom, a.geom)
  AND g.is_land
ORDER BY g.child_id;
```

```
 child_id | parent_id |  lat  |  lon   | area_weight
 BQ2C     | 0QCL      | -4.75 | -51.00 |      0.0167
 BR6D     | 0QML      | -5.00 | -50.75 |      0.1116
 BSAD     | 0QML      | -5.25 | -50.75 |      0.1116
 ...                                     (16 cells)
```

`g.geom && a.geom` is the cheap bounding-box test that uses the index; `ST_Intersects` is the
exact one. Keeping both is the standard PostGIS idiom. Note `ST_Area` here is in square degrees —
fine as a *relative* weight at a single latitude, but if your polygon spans many degrees of
latitude, weight on a projected area instead (`ST_Area(ST_Transform(..., 3857))` or better, an
equal-area CRS).

For a real administrative boundary, load the shapefile into a table and join to it instead of
pasting WKT.

### 8b. One `.WTH` per cell in the polygon

The gridded-simulation case: each cell becomes its own DSSAT station.

```bash
#!/usr/bin/env bash
set -euo pipefail
YR=2020
WKT='POLYGON((-51.0 -5.5, -50.2 -5.5, -50.2 -4.8, -51.0 -4.8, -51.0 -5.5))'
PSQL="docker compose exec -T postgres psql -U era5 -d era5 -qtA"

$PSQL -c "SELECT g.child_id || ' ' || g.parent_id
          FROM era5_land_base_grid g
          WHERE ST_Intersects(g.geom, ST_GeomFromText('$WKT', 4326)) AND g.is_land" \
| while read -r child parent; do
    $PSQL -v child="$child" -v parent="$parent" -v yr="$YR" -f wth_by_code.sql \
        > "${child}${YR}.WTH"
    echo "wrote ${child}${YR}.WTH"
  done
```

`wth_by_code.sql` is Recipe 2 with the first block replaced — it takes codes directly instead of
resolving them from a coordinate:

```sql
-- header of wth_by_code.sql: expects -v child=... -v parent=... -v yr=...
WITH cell AS (
    SELECT g.child_id, g.parent_id, g.lat, g.lon, g.elevation
    FROM era5_land_base_grid g WHERE g.child_id = :'child'
), ...
```

One process per cell is deliberate: each query prunes to one partition and returns in
milliseconds, and a failure leaves the other files intact.

### 8c. One `.WTH` for the whole polygon (area-weighted mean)

The field-scale or municipality case: a single station representing the area.

```sql
WITH aoi AS (
    SELECT ST_GeomFromText(
      'POLYGON((-51.0 -5.5, -50.2 -5.5, -50.2 -4.8, -51.0 -4.8, -51.0 -5.5))', 4326) AS geom
), cells AS (
    SELECT g.child_id, g.parent_id,
           ST_Area(ST_Intersection(g.geom, a.geom)) AS w
    FROM era5_land_base_grid g
    JOIN aoi a ON g.geom && a.geom
    WHERE ST_Intersects(g.geom, a.geom) AND g.is_land
)
SELECT w.date,
       sum(w.srad   * c.w) / sum(c.w) AS srad,
       sum(w.tmax   * c.w) / sum(c.w) AS tmax,
       sum(w.tmin   * c.w) / sum(c.w) AS tmin,
       sum(w.precip * c.w) / sum(c.w) AS rain,
       sum(w.tdew   * c.w) / sum(c.w) AS tdew,
       sum(w.wind   * c.w) / sum(c.w) AS wind,
       sum(w.rh     * c.w) / sum(c.w) AS rh,
       count(*)                       AS n_cells
FROM wth_base w
JOIN cells c ON c.parent_id = w.parent_id AND c.child_id = w.child_id
WHERE w.parent_id IN (SELECT DISTINCT parent_id FROM cells)   -- partition pruning
  AND w.date BETWEEN '2020-01-01' AND '2020-12-31'
GROUP BY w.date
ORDER BY w.date;
```

Wrap that in Recipe 2's formatting and header scaffold, using the polygon centroid for
`LAT`/`LONG` and the weighted mean elevation for `ELEV`. Measured: 16 cells × 366 days in 12.5 s —
roughly 50× the single-cell query for 16× the data, because the polygon spans two partitions and
the spatial join is paid per cell.

Three warnings about spatial averaging:

- **Never average `.WTH` files that were already written.** Average the silver rows, then write
  one file. Averaging derived fields after the fact is not the same operation.
- **`RAIN` is the field averaging hurts most.** Averaging 16 cells turns 16 convective storms into
  16 days of drizzle; totals survive, daily intensity does not. For rain-fed crop simulation this
  systematically changes the water balance. Prefer 8b (one file per cell) when rainfall matters.
- **`ET0` and `RH` are non-linear in their inputs.** The rows in `wth_base` already hold ET0
  computed per cell, so averaging those is defensible; recomputing ET0 from averaged inputs is
  not the same number. Do not mix the two approaches within one study.

Non-linear averaging problems apply equally when the average `n_cells` changes over time — check
that `count(*)` is constant across dates before trusting a trend.

### 8d. Many named sites at once

For a site list rather than a polygon, feed coordinates as a `VALUES` list and let the encoder do
the rest:

```sql
WITH sites(name, lat, lon) AS (
    VALUES ('Palmas',    -10.24, -48.35),
           ('Balsas',     -7.53, -46.03),
           ('Sorriso',   -12.54, -55.71)
)
SELECT s.name, s.lat, s.lon,
       era5_child_id(s.lat, s.lon)     AS child_id,
       era5_parent_id(s.lat, s.lon, 4) AS parent_id,
       g.lat AS cell_lat, g.lon AS cell_lon, g.elevation
FROM sites s
LEFT JOIN era5_land_base_grid g ON g.child_id = era5_child_id(s.lat, s.lon)
ORDER BY s.name;
```

Two sites closer than 0.25° will land in the same cell and get the same `child_id` — and therefore
the same filename. That is the encoding telling you the truth about its resolution, not a
collision to work around.

---

## 9. Recipe 6 — preflight QA (run this before writing files)

A `.WTH` with silent gaps produces a simulation that runs and is wrong.

```sql
\set child 'BSAD'
\set parent '0QML'
\set yr 2020

-- 1. Completeness and provenance in one row
SELECT count(*)                                             AS days_present,
       (make_date(:yr,12,31) - make_date(:yr,1,1) + 1)      AS days_expected,
       count(*) FILTER (WHERE tmax IS NULL OR tmin IS NULL) AS null_temp,
       count(*) FILTER (WHERE srad IS NULL)                 AS null_srad,
       count(*) FILTER (WHERE precip IS NULL)               AS null_rain,
       count(*) FILTER (WHERE is_preliminary)               AS era5t_rows,
       count(*) FILTER (WHERE imputed <> 0)                 AS repaired_rows
FROM wth_base
WHERE parent_id = :'parent' AND child_id = :'child'
  AND date BETWEEN make_date(:yr,1,1) AND make_date(:yr,12,31);

-- 2. Exactly which days are missing
SELECT d::date AS missing_date
FROM generate_series(make_date(:yr,1,1), make_date(:yr,12,31), interval '1 day') d
WHERE NOT EXISTS (
    SELECT 1 FROM wth_base w
    WHERE w.parent_id = :'parent' AND w.child_id = :'child' AND w.date = d::date);

-- 3. Which variables were repaired, and from what
SELECT date, variable, method, original_value, new_value
FROM wth_imputation_log
WHERE parent_id = :'parent' AND child_id = :'child'
  AND date BETWEEN make_date(:yr,1,1) AND make_date(:yr,12,31)
ORDER BY date;

-- 4. Cell-days quarantined by QA — these are ABSENT from wth_base, so a gap in query 2
--    with a row here is explained, not mysterious
SELECT date, reason FROM wth_qa_failures
WHERE parent_id = :'parent' AND child_id = :'child'
  AND date BETWEEN make_date(:yr,1,1) AND make_date(:yr,12,31)
ORDER BY date;
```

Interpreting the flags:

- **`era5t_rows > 0`** — those days are preliminary ERA5T, not final ERA5. Expect them in roughly
  the last 3 months; the `update` DAG replaces them. Re-export after it runs.
- **`repaired_rows > 0`** — `imputed` is a bitmask, not a boolean, so it tells you *which*
  variable was filled: `tmax=1, tmin=2, precip=4, srad=8, wind=16, tdew=32, rh=64, et0=128`.
  Test one variable with `imputed & 4 <> 0` (precip repaired). Sum the bits to read a combination.
- **`wth_data_issues`** is the field-wide registry — if a whole variable-date was defective across
  many cells, the story is there, with its `status` and `resolution`:

  ```sql
  SELECT variable, date, detector, cells, status, resolution
  FROM wth_data_issues
  WHERE date BETWEEN '2020-01-01' AND '2020-12-31'
  ORDER BY date;
  ```

A quick sanity check on the finished file, independent of the database — the ET0 column silver
already computed should track a reasonable range, and `tmax >= tmin` must hold everywhere:

```sql
SELECT count(*) AS impossible_rows
FROM wth_base
WHERE parent_id = :'parent' AND child_id = :'child'
  AND date BETWEEN make_date(:yr,1,1) AND make_date(:yr,12,31)
  AND (tmax < tmin OR precip < 0 OR rh < 0 OR rh > 100 OR srad < 0);
```

This should be `0` — the QA node already enforces it — but running it costs nothing and catches a
hand-edited file or a mis-typed conversion.

---

## 10. Gotchas, ranked by how much time they cost

| # | Symptom | Cause | Fix |
|---|---|---|---|
| 1 | Query hangs for minutes, then times out | `parent_id` not a literal at plan time — Postgres cannot prune 10k+ partitions | Resolve codes with `\gset` first, then use `:'parent'` as a literal. **Measured: the same query went from a 2-minute timeout to 0.24 s.** |
| 2 | Coordinates on a `.125` boundary give the wrong cell | Postgres `round()` is half-away-from-zero; Python's is banker's rounding | Use `round_half_even` (already in the section 3 block). Only matters exactly on cell edges — but it silently picks the neighbouring cell when it matters. |
| 3 | Wind values look 13× too large | `wind` is m/s at 10 m; DSSAT wants km/day at 2 m | `× 64.6272`, and set `WNDHT` to `2.0`, not `10.0` |
| 4 | ERA5 and CHIRPS rainfall disagree day by day | Different 24-hour windows (local day vs product day) — not a measurement disagreement | Compare monthly totals; never "correct" one to the other |
| 5 | `chirps_era5_map` returns nothing | The map is built per-extent and is empty on a fresh database | `uv run python -m src.db.chirps_map` |
| 6 | `.WTH` has column headers or `(366 rows)` in it | `psql` ran without `-qtA` | Use `-qtA`; check the last line of every generated file |
| 7 | Fine-grid query returns nothing for a valid coordinate | `chirps_base_grid` is region-scoped — only built extents exist | Check `SELECT count(*) FROM chirps_base_grid WHERE fine_id = chirps_fine_id(lat, lon)` |
| 8 | `function chirps_fparent_of(character) does not exist` | `CHAR(n)` argument, function takes `TEXT` | Cast: `chirps_fparent_of(m.fine_id::text)`, or `rtrim()` it — `CHAR` is blank-padded, which also breaks equality comparisons |
| 9 | `.5` instead of `0.5` in a column | `to_char` mask without a leading `0` | Use `FM99990.0`, not `FM9999.9` |
| 10 | Two sites produced the same filename | They are in the same 0.25° cell | Expected. Either accept one station, or use the 0.05° CHIRPS grid where only rainfall needs the detail |

---

## 11. Where to go next

- **`sql/wth_helpers.sql`** — every recipe above, packaged as callable functions. Import once.
- **`scripts/wth_export.py`** — the psycopg2 client over those functions, with a CLI.
- **`PLANNING.md` §9** — the authoritative gold spec these recipes implement by hand.
- **`docs/runbook.md`** — how the data got there in the first place.
- **`src/db/query.py` / `src/db/precip_query.py`** — the Python equivalents of Recipes 1, 4 and 5.
  If you are pulling data into pandas rather than writing files, start there instead.
- **`src/db/chirps_map.py`** — the area-weighting and the `precip_compare` view.
- **`src/transform/et0.py`** — FAO-56 Penman-Monteith, if you need to know exactly what `et0`
  means before using it as a check.

When the gold module is implemented, it should produce byte-identical output to Recipe 2. This
document is the reference for that test.
