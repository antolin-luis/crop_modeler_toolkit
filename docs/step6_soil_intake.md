# Step 6 — SoilGrids-for-DSSAT point intake

> Adds the soil side of a DSSAT run. Loads a global 5 arc-min soil-profile point layer
> into PostGIS and bridges it to the 0.25° weather grid, so a caller holding a `child_id`
> can pull a `.WTH` and its matching `ID_SOIL` from one join. Depends on Step 2 (the grid);
> independent of bronze/silver.

## Context

Everything built so far produces weather. A DSSAT experiment also needs a soil profile,
named in the FILEX's `ID_SOIL` field and defined in a `.SOL` file. Until now that pairing
was the user's problem, done by hand per site.

`Point5m_SoilGrids-for-DSSAT-10km_v1.shp` supplies it globally: SoilGrids soil data already
processed into DSSAT profiles, one per 5 arc-min (~10 km) land cell, with the profile
identity carried as a point attribute.

## The source

| | |
|---|---|
| Records | 1,984,797 point features, global land |
| CRS | `GCS_WGS_1984` (EPSG:4326) |
| Resolution | 5 arc-min (~10 km) |
| DBF | 1.1 GB uncompressed, 556-byte fixed records, dBase III |
| Archive | 36 MB zipped |

Attributes:

| Field | DBF type | Meaning |
|---|---|---|
| `CELL5M` | `N(9)` | HarvestChoice 5 arc-min cell id — the source's own key |
| `SoilProfil` | `C(254)` | DSSAT `ID_SOIL`, e.g. `AD02455938` |
| `X`, `Y` | `F(19,11)` | point longitude / latitude |
| `ISO2` | `C(254)` | country, 225 distinct values |

Two findings worth stating plainly, because both are easy to get wrong:

- **`SoilProfil` is a profile ID, not a filename.** Every value is `ISO2` + a zero-padded
  8-digit `CELL5M`, and all 1,984,797 of them are exactly 10 characters. It is what goes in
  a FILEX; the `.SOL` file that holds the profile's horizons is presumably `<ISO2>.SOL` and
  is **not** ingested here. Only the identity is stored.
- **`X`/`Y` are attributes**, so the 55 MB `.shp` carries nothing the `.dbf` does not. The
  geometry half of the shapefile is never opened.

## Why a hand-rolled DBF reader

The Airflow image has no `geopandas`, `fiona`, `pyogrio`, `osgeo`, or `ogr2ogr` — only
`shapely` and `rasterio` (a raster wheel). Reading this file the conventional way means
adding GDAL wheels to `airflow/Dockerfile` and rebuilding on a Raspberry Pi 5.

`src/grid/dbf.py` is ~200 lines instead. dBase III is a 32-byte header plus a 32-byte
descriptor per field and fixed-width records; there is no ambiguity to lose. The project
already made this trade once — `seed_grid.assign_timezone` uses `timezonefinder` rather
than a geopandas point-in-polygon join, for the same target hardware.

The reader is streaming and columnar: the DBF member is read straight out of the zip
without ever being extracted, in chunks, and each chunk is viewed as a numpy structured
array so per-record work stays in C rather than a two-million-iteration Python loop. Peak
memory is set by `chunk_rows`, not by file size. Unsupported DBF type codes raise rather
than being guessed at; `columns=` lets a caller select around one.

Measured on the Pi 5: **18 s** to parse all 1,984,797 records and encode their cell ids.

## Tables

`src/db/soil_schema.sql`, applied at run time by its loader (the convention every
non-shipped table follows — `schema.sql` only runs at initdb).

**`soil_profile_points`** — one row per source point: `cell5m` (PK), `soil_id`, `iso2`,
`lat`, `lon`, `child_id`, `parent_id`, `geom`. 397 MB with indexes.

**`soil_era5_map`** — one row per (ERA5 cell, soil point) pair: `child_id`, `cell5m`,
`soil_id`, `dist_deg`, `is_nearest`. 179 MB.

`soil_id` and `iso2` are `VARCHAR`, deliberately not `CHAR(n)`: psycopg2 returns `CHAR(n)`
space-padded and every consumer then has to remember `.str.strip()`. Neither is a
fixed-width code space, so neither pays that tax.

## Design decisions

**`child_id` is arithmetic, not spatial.** Each point is snapped to the nearest 0.25° cell
centre and encoded with the same vectorized `cell_codes`/`parent_codes` the grid itself was
built with. The snap is mandatory — `encoding.cell_code`'s contract is that inputs *are*
centres, and a 5 arc-min point almost never is. Skip it and every `child_id` is still a
valid-looking code, just the wrong cell. `np.rint` is banker's rounding, matching the scalar
encoders' `round` exactly on a boundary; longitude wraps modulo `NLON` rather than clipping,
so a point just west of the prime meridian lands at index 0 and not at 1439.

**`geom` is built in SQL**, via `ST_MakePoint` in the final `INSERT ... SELECT` — two
million Python WKT strings are waste when Postgres already has the numbers. The GiST index
exists for real polygon queries, the same role it plays on the grid.

**`soil_era5_map` is an equality join, no PostGIS.** The cell assignment is already stored,
so a spatial join would re-derive it more slowly and hide the code-space contract.
`dist_deg` scales the longitude term by `cos(lat)` so "nearest" means nearest on the ground,
which matters at high latitude where a 0.25° cell is a narrow sliver; `cell5m` breaks ties
so a rebuild is deterministic.

**Global, truncate-and-load.** Unlike `chirps_grid_build` this is not extent-scoped. The
whole layer is ~2 M rows / 576 MB, small enough that scoping it would only create a "which
regions did I build?" question with no upside. The load is one transaction, so a failure
halfway leaves the previous table intact.

## Files

| File | Role |
|---|---|
| `src/grid/dbf.py` | streaming dBase III reader |
| `src/db/soil_schema.sql` | DDL for both tables |
| `src/db/soil_helpers.sql` | coordinate-lookup SQL functions, installed by the DAG |
| `src/db/seed_soil.py` | `build_rows` / `load_points` / `install_helpers` / `build_map` / `validate` + CLI |
| `src/db/soil_query.py` | Python point lookup + CLI |
| `airflow/dags/soil_grid_build.py` | `load_points >> install_helpers >> build_map >> validate` |
| `tests/test_dbf.py` | DBF bytes built in memory; every conversion pinned |
| `tests/test_seed_soil.py` | `build_rows` snapping, emitted SQL, path resolution |
| `tests/test_soil_query.py` | `locate` against real ids, PK-not-PostGIS, nearest fallback |

No new dependencies, no image rebuild, no Compose change.

## Running it

The archive must sit under `$DATA_DIR/bronze/static/` — only `$DATA_DIR` is bind-mounted
into the Airflow containers, so a file left at the repo root is invisible there. That
directory is owned by the `airflow` uid, so copy it in through the container:

```bash
docker compose cp Point5m_SoilGrids-for-DSSAT-10km_v1.shp.zip \
  airflow-scheduler:/data/bronze/static/
```

Then trigger `soil_grid_build` with default params, or run the CLI directly:

```bash
docker compose exec airflow-scheduler python -m src.db.seed_soil
```

`era5_land_base_grid` must be populated first (normally it already is — the shipped seed
restores at first boot). `build_map` fails loudly rather than writing an empty bridge.

Measured end-to-end on the Pi 5: `load_points` 80 s, `install_helpers` 1 s, `build_map`
13 s, `validate` 3 s.

## Verification

The `validate` task prints, and the values from the first live run:

```
points: 1984797
countries: 225
soil_id_lengths: {10: 1984797}
orphan_points: 0
map_rows: 1984797
era5_cells_covered: 236764
```

`orphan_points` is the one that raises: `child_id` is a pure function of the coordinates and
the grid is global, so a point that fails to join means the encoders and the grid disagree —
a bug, not bad input.

The join the whole step exists for:

```sql
SELECT m.child_id, m.soil_id, g.lat, g.lon
FROM soil_era5_map m
JOIN era5_land_base_grid g USING (child_id)
WHERE m.is_nearest AND g.is_land;
```

Coverage sanity: 236,764 ERA5 cells hold soil points, averaging 8.38 points per cell
(max 16) — as expected for 3×3 five-arc-minute cells inside a 0.25° cell, minus coastline.

## Looking up a profile from a pair of coordinates

`src/db/soil_query.py` (Python) and `src/db/soil_helpers.sql` (the same thing for DBeaver).

The SQL functions are installed by the DAG's `install_helpers` task, so a completed run
leaves a database that answers `SELECT * FROM soil_id_at(lat, lon)` with nothing further to
import. That is why the file sits in `src/db/` rather than with the other DBeaver scripts in
`sql/`: only `./src` and `./airflow/dags` are mounted into the Airflow containers, so a file
in `sql/` is invisible to the DAG. It is still an ordinary script — import it by hand if you
edit a function. It must run **after** `load_points`, because Postgres parse-analyzes a
SQL-language function body at CREATE time and every one of these references
`soil_profile_points`.

The source points sit on a regular 5 arc-min grid, so a coordinate resolves to its
`cell5m` **arithmetically** and the lookup is a primary-key read — no spatial index, one
row. The encoding was verified against the loaded table, not assumed:

```
cell5m = floor((90 - lat) * 12) * 4320 + floor((lon + 180) * 12)
```

Row-major from the north pole, 4320 columns, 0-indexed, no offset. Zero mismatches on
every sampled row of the loaded table. Note it is a **containing-cell** rule (floor), not
nearest-centre (round) as on the weather side — a coordinate belongs to the cell whose box
it falls in, which is what the source ids mean.

```python
from src.db.soil_query import profile_at

p = profile_at(-34.9, -56.2)
# SoilProfile(cell5m=6472845, soil_id='UY06472845', iso2='UY',
#             lat=-34.875, lon=-56.208, child_id='FGHR', parent_id='0YYF', dist_km=0.0)
```

```sql
SELECT * FROM soil_id_at(-34.9, -56.2);
```

```bash
docker compose exec airflow-scheduler python -m src.db.soil_query --lat -34.9 --lon -56.2
```

The layer is land-only, so a coastal, lake or ice coordinate can land on a cell with no
profile. Two behaviours, and the caller picks:

- **exact** (`nearest=False`, or `soil_profile_at`) — a miss is a miss, `None` / zero rows.
- **nearest** (the default, `max_km=25`) — the closest profile within the radius, with
  `dist_km` reported so the caller can judge whether a profile 7 km away suits them. This
  is the one place `soil_profile_points.geom` and its GiST index earn their keep. Distance
  is measured on the geography in true metres; the bounding-box prefilter is widened in x
  by `1/cos(lat)`, because longitude degrees shrink toward the poles and a square degree
  box would under-reach and report "no profile" where one sits well inside the radius.

Measured behaviour: Montevideo (−34.9, −56.2) hits exactly; a point 5 km offshore in the
Río de la Plata misses its cell and falls back to `UY06477166` at 6.91 km; mid-Atlantic
(0, −30) returns nothing at all.

`profiles_at(coords)` does a whole batch in one query, exact-cell only, returning a frame
in the input order with NULLs for the misses.

**One trap, worth naming because it bit this file in review.** In a SQL-language function,
a bare `lat` inside a query over `soil_profile_points` resolves to the *table's* `lat`
column, not to the parameter of the same name. `WHERE s.cell5m = soil_cell5m(lat, lon)`
silently became `s.cell5m = s.cell5m` — true for every row, and the function returned
whichever row it scanned first, identically for every input. Every parameter in
`src/db/soil_helpers.sql` is therefore prefixed `p_`. Do not un-prefix them.

## Not built here

- `.SOL` file contents. Only profile identity is stored; horizon data stays outside the
  database.
- Any gold-layer wiring. `scripts/wth_export.py` does not yet emit an `ID_SOIL` alongside
  the `.WTH` it writes; this step supplies the table that would let it.
