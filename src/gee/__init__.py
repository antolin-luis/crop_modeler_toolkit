"""Google Earth Engine bronze backend — an alternative to ``src/cds/``.

GEE serves ERA5 from hot storage (no MARS tape, no queue) and reduces hourly→daily
*server-side*, so only the daily result is exported. It mirrors the CDS package and shares
the canonical grid encoder (``src/grid/encode_long``), the idempotent ``Manifest``, and the
bronze parquet schema, so a ``(variable, year)`` fetched by either backend is the same file.

Setup (noncommercial / student): see ``docs/gee_setup.md``.
"""
