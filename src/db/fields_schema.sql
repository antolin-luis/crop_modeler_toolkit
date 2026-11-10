-- field_parcel / s2_pixel / s2_ndvi_obs / s2_ndvi_chunk DDL: per-field Sentinel-2 NDVI.
--
-- Kept out of schema.sql for the same reason soil_schema.sql is: schema.sql ships with the
-- grid seed and runs once at first boot. These are created by the s2_ndvi_fields DAG, so
-- an existing database picks them up with no volume reset.
--
-- No bronze layer. A field-year of NDVI is a few MB pulled synchronously from Earth
-- Engine (computePixels) and written straight here; re-pulling it is cheaper than
-- keeping a Parquet copy in sync.
--
-- Pixel identity is a pure function of coordinates, like the ERA5 grid: (epsg, ix, iy)
-- is the 10 m Sentinel-2 cell whose lower-left corner is (ix*10, iy*10) in that UTM zone.
-- S2 tiles put pixel edges on multiples of 10 m, so this is the native grid, not a
-- resample. `pixel_id` is a surrogate only to keep s2_ndvi_obs rows narrow.

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS field_parcel (
    field_id    SERIAL PRIMARY KEY,
    field_set   VARCHAR(64)  NOT NULL,      -- one shapefile = one set, e.g. 'Weizen_LG_E2026'
    src_id      VARCHAR(64)  NOT NULL,      -- the shapefile's own id column, as text
    name        VARCHAR(128),
    attrs       JSONB        NOT NULL,      -- every DBF column, verbatim
    area_ha     REAL         NOT NULL,      -- geodesic, from geom
    geom        GEOMETRY(MultiPolygon, 4326) NOT NULL,
    loaded_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (field_set, src_id)
);

CREATE INDEX IF NOT EXISTS idx_field_parcel_geom ON field_parcel USING GIST (geom);

-- A pixel belongs to the field that contains its centre — the same rule rasterizing a
-- polygon uses, so each pixel lands in at most one field.
CREATE TABLE IF NOT EXISTS s2_pixel (
    pixel_id    SERIAL PRIMARY KEY,
    field_id    INTEGER NOT NULL REFERENCES field_parcel (field_id) ON DELETE CASCADE,
    epsg        INTEGER NOT NULL,           -- UTM zone of the field centroid, 326xx / 327xx
    ix          INTEGER NOT NULL,           -- floor(easting / 10)
    iy          INTEGER NOT NULL,           -- floor(northing / 10)
    geom        GEOMETRY(Point, 4326) NOT NULL,   -- pixel centre
    UNIQUE (epsg, ix, iy)
);

CREATE INDEX IF NOT EXISTS idx_s2_pixel_field ON s2_pixel (field_id);

-- Clear observations only: a row exists when Cloud Score+ cs_cdf >= the threshold the
-- chunk was extracted with. cs_cdf is kept so a consumer can tighten it, never loosen it.
-- Same-day tile overlaps are already collapsed (best cs_cdf wins), so (pixel, date) is
-- unique.
CREATE TABLE IF NOT EXISTS s2_ndvi_obs (
    pixel_id    INTEGER NOT NULL REFERENCES s2_pixel (pixel_id) ON DELETE CASCADE,
    date        DATE    NOT NULL,           -- UTC acquisition date
    ndvi        REAL    NOT NULL,
    cs_cdf      REAL    NOT NULL,
    PRIMARY KEY (pixel_id, date)
);

-- Done-manifest, one row per (field, calendar year). A rerun skips a chunk whose stored
-- window already covers the requested one at a threshold no stricter than requested.
CREATE TABLE IF NOT EXISTS s2_ndvi_chunk (
    field_id     INTEGER NOT NULL REFERENCES field_parcel (field_id) ON DELETE CASCADE,
    year         INTEGER NOT NULL,
    start_date   DATE    NOT NULL,          -- inclusive
    end_date     DATE    NOT NULL,          -- exclusive
    cs_threshold REAL    NOT NULL,
    n_dates      INTEGER NOT NULL,
    n_obs        INTEGER NOT NULL,
    done_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (field_id, year)
);
