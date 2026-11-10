"""Build DSSAT ``.WTH`` files from the silver database, from Python.

The psycopg2 companion to ``sql/wth_helpers.sql`` and ``docs/wth_from_sql.md``. Gold
(``.WTH`` materialization) is still deferred in the pipeline (PLANNING.md §9), so this is
the interim path: point at a coordinate or a polygon, pick a rainfall source, get files.

**It calls the SQL functions rather than re-implementing them.** Every formula — the wind
conversion, the ``TAV``/``AMP`` climatology, the column widths, the CHIRPS join — lives in
``sql/wth_helpers.sql`` and nowhere else. This module is a typed client over those
functions, so the DBeaver path and the Python path cannot drift apart. Verified: the file
produced here is byte-identical to the one ``psql`` produces from the same functions.

Install the functions once (idempotent, read-only, creates no tables)::

    uv run python scripts/wth_export.py install

Then, from the shell::

    uv run python scripts/wth_export.py file    -5.175 -50.725 2020 --out .
    uv run python scripts/wth_export.py file    -5.175 -50.725 2020 --rain chirps_v2 --out .
    uv run python scripts/wth_export.py header  -5.175 -50.725
    uv run python scripts/wth_export.py body    -5.175 -50.725 2020 --rain chirps_v3_rnl
    uv run python scripts/wth_export.py qa      -5.175 -50.725 2020
    uv run python scripts/wth_export.py compare -5.175 -50.725 2020
    uv run python scripts/wth_export.py polygon "POLYGON((...))" 2020 --out ./wth

Or from Python::

    from scripts.wth_export import connect, wth_text, write_wth, body, header

    with connect() as conn:
        print(wth_text(conn, -5.175, -50.725, 2020, rain="chirps_v2"))
        path = write_wth(conn, -5.175, -50.725, 2020, out_dir="./wth")

**Connection.** ``.env`` sets ``POSTGRES_HOST=postgres``, which is the container-network
name and does not resolve from the host machine. :func:`connect` tries the configured host
and falls back to ``localhost`` when the name cannot be resolved, so the same code works
inside Airflow and from a laptop. Override explicitly with ``WTH_PG_HOST`` or ``host=``.
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# The canonical Python encoders. Used for coordinate -> code arithmetic that needs no
# database round-trip; they are also what the database itself was built with, so the SQL
# functions in sql/wth_helpers.sql are validated against them.
from src.grid.encoding import cell_code, code_to_latlon, parent_code
from src.grid.fine_encoding import fine_code, fine_parent_code

HELPERS_SQL = Path(__file__).resolve().parents[1] / "sql" / "wth_helpers.sql"

# Must match the b the database was seeded with (immutable, PLANNING.md §6.3).
BLOCK_B = 4

RAIN_SOURCES = ("era5", "chirps_v2", "chirps_v3_rnl", "chirps_v3_sat")
CHIRPS_MODES = ("point", "weighted")

# The functions this module depends on. Checked up front so a missing install fails with
# an instruction instead of a bare "function does not exist" from three frames down.
REQUIRED_FUNCTIONS = (
    "era5_child_id",
    "era5_parent_id",
    "wth_body",
    "wth_header",
    "wth_file",
    "wth_qa",
)


# --------------------------------------------------------------------------------
# Connection
# --------------------------------------------------------------------------------


def dsn(host: str | None = None) -> str:
    """Build a Postgres DSN from ``.env`` / the environment.

    Host precedence: the ``host`` argument, then ``WTH_PG_HOST``, then ``POSTGRES_HOST``,
    then ``localhost``. Deliberately does not go through ``src.config.load_config()``:
    that one also requires ``CDS_KEY``, which has nothing to do with reading the database
    and should not be able to block an export.
    """
    load_dotenv()
    host = host or os.getenv("WTH_PG_HOST") or os.getenv("POSTGRES_HOST") or "localhost"
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "era5")
    user = os.getenv("POSTGRES_USER", "era5")
    password = os.getenv("POSTGRES_PASSWORD")
    if not password:
        raise RuntimeError("POSTGRES_PASSWORD is not set (see .env / .env.example)")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def connect(host: str | None = None, *, quiet: bool = False):
    """Open a psycopg2 connection, falling back to ``localhost`` if the host is a
    container name that does not resolve from here.

    The fallback exists because ``.env`` legitimately holds ``POSTGRES_HOST=postgres`` for
    the compose network; without it every host-side run would need an env override, and
    the failure ("could not translate host name") reads like a database outage.
    """
    try:
        return psycopg2.connect(dsn(host))
    except psycopg2.OperationalError as exc:
        resolvable = "could not translate host name" not in str(exc)
        if resolvable or host is not None:
            raise
        if not quiet:
            print(
                "note: POSTGRES_HOST is a container name and does not resolve here; "
                "retrying on localhost:5432",
                file=sys.stderr,
            )
        return psycopg2.connect(dsn("localhost"))


def helpers_installed(conn) -> list[str]:
    """Return the required SQL functions that are **missing**. Empty list means ready."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT proname FROM pg_proc WHERE proname = ANY(%s)",
            (list(REQUIRED_FUNCTIONS),),
        )
        present = {row[0] for row in cur.fetchall()}
    return [f for f in REQUIRED_FUNCTIONS if f not in present]


def install_helpers(conn, script: Path = HELPERS_SQL) -> None:
    """Run ``sql/wth_helpers.sql``. Idempotent — every function is CREATE OR REPLACE.

    Creates functions only: no tables, no data touched. Safe on a live database.
    """
    sql_text = script.read_text()
    with conn.cursor() as cur:
        cur.execute(sql_text)
    conn.commit()


def _require_helpers(conn) -> None:
    missing = helpers_installed(conn)
    if missing:
        raise RuntimeError(
            f"missing SQL helpers: {', '.join(missing)}. Install them once with:\n"
            f"    uv run python scripts/wth_export.py install\n"
            f"or:  docker compose exec -T postgres psql -U era5 -d era5 -f - "
            f"< {HELPERS_SQL.relative_to(Path.cwd()) if HELPERS_SQL.is_relative_to(Path.cwd()) else HELPERS_SQL}"
        )


# --------------------------------------------------------------------------------
# Coordinates -> cells (pure arithmetic, no database)
# --------------------------------------------------------------------------------


@dataclass(frozen=True)
class Cell:
    """Where a coordinate lands on both grids.

    ``child_id`` doubles as the DSSAT station code, so a ``.WTH`` filename decodes back to
    a coordinate with no lookup table (PLANNING.md §6.2). ``lat``/``lon`` are the 0.25°
    cell CENTRE — report that, not the requested coordinate, since it is where the data is.
    """

    child_id: str
    parent_id: str
    fine_id: str
    fparent_id: str
    lat: float
    lon: float

    @property
    def filename_stem(self) -> str:
        return self.child_id


def locate(lat: float, lon: float) -> Cell:
    """Resolve a coordinate on both grids. No database, no PostGIS, no spatial index."""
    child = cell_code(lat, lon)
    clat, clon = code_to_latlon(child)
    return Cell(
        child_id=child,
        parent_id=parent_code(lat, lon, BLOCK_B),
        fine_id=fine_code(lat, lon),
        fparent_id=fine_parent_code(lat, lon),
        lat=clat,
        lon=clon,
    )


# --------------------------------------------------------------------------------
# Header
# --------------------------------------------------------------------------------


@dataclass(frozen=True)
class Header:
    """The ``.WTH`` station line, as numbers.

    ``elev`` is derived from geopotential (``z / 9.80665``) on the 0.25° model orography —
    not a high-res DEM, because silver's ET0 was computed against this one.

    ``amp`` is HALF the annual range of the monthly mean temperatures. Some tooling uses
    the full range; a 2× error here shifts soil temperature and therefore phenology, so
    state which convention a dataset used.

    ``wndht`` is 2.0, not 10.0: the wind column was already adjusted 10 m → 2 m by the
    FAO-56 0.748 factor. Writing 10.0 double-counts the correction.

    ``n_months`` below 12 means the climatology window is thin and ``tav``/``amp`` are
    built on partial data.
    """

    insi: str
    child_id: str
    parent_id: str
    lat: float
    lon: float
    elev: float | None
    tav: float | None
    amp: float | None
    refht: float
    wndht: float
    t_zone: int
    n_months: int


def header(
    conn,
    lat: float,
    lon: float,
    *,
    clim_from: int = 1991,
    clim_to: int = 2020,
) -> Header:
    """Header fields for the cell containing ``(lat, lon)``.

    The climatology window defaults to the 1991-2020 WMO normal rather than the simulated
    year, so the header describes the site instead of one year's weather.
    """
    _require_helpers(conn)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT insi, child_id, parent_id, lat, lon, elev, tav, amp, refht, wndht,"
            " t_zone, n_months FROM wth_header(%s, %s, %s, %s)",
            (lat, lon, clim_from, clim_to),
        )
        row = cur.fetchone()
    if row is None:
        raise LookupError(
            f"no grid cell for ({lat}, {lon}) — outside the seeded grid, or the grid "
            "table is empty"
        )
    insi, child, parent, clat, clon, elev, tav, amp, refht, wndht, tz, n = row
    return Header(
        insi=insi.strip(),
        child_id=child.strip(),
        parent_id=parent.strip(),
        lat=clat,
        lon=clon,
        elev=elev,
        tav=tav,
        amp=amp,
        refht=refht,
        wndht=wndht,
        t_zone=tz,
        n_months=n,
    )


def header_lines(
    conn,
    lat: float,
    lon: float,
    year: int,
    *,
    rain: str = "era5",
    clim_from: int = 1991,
    clim_to: int = 2020,
) -> list[str]:
    """The four formatted header lines (description, blank, ``@ INSI...``, values)."""
    _require_helpers(conn)
    _check_rain(rain)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT wth_header_lines(%s, %s, %s, %s, %s, %s)",
            (lat, lon, year, rain, clim_from, clim_to),
        )
        return [r[0] for r in cur.fetchall()]


# --------------------------------------------------------------------------------
# Body
# --------------------------------------------------------------------------------


@dataclass(frozen=True)
class BodyRow:
    """One daily record, in DSSAT units.

    ``wind`` is already km/day at 2 m. ``rain_coverage`` is 1.0 for ERA5 and for the
    single-fine-cell CHIRPS mode; under ``chirps_mode="weighted"`` it is the fraction of
    the ERA5 cell the fine grid actually covers — below 1.0 means the mean is over part of
    the cell, which is normal at the edge of a built extent and worth knowing.

    ``imputed`` is a bitmask, not a boolean: ``tmax=1 tmin=2 precip=4 srad=8 wind=16
    tdew=32 rh=64 et0=128``. ``is_preliminary`` marks ERA5T rather than final ERA5.
    """

    date: date
    srad: float | None
    tmax: float | None
    tmin: float | None
    rain: float | None
    dewp: float | None
    wind: float | None
    rhum: float | None
    rain_coverage: float | None
    is_preliminary: bool
    imputed: int


def _check_rain(rain: str) -> None:
    if rain.lower() not in RAIN_SOURCES:
        raise ValueError(f"rain must be one of {RAIN_SOURCES}, got {rain!r}")


def _check_mode(mode: str) -> None:
    if mode.lower() not in CHIRPS_MODES:
        raise ValueError(f"chirps_mode must be one of {CHIRPS_MODES}, got {mode!r}")


def body(
    conn,
    lat: float,
    lon: float,
    year: int,
    *,
    rain: str = "era5",
    chirps_mode: str = "point",
) -> list[BodyRow]:
    """Daily rows for one cell-year.

    ``rain``:
      ``era5``           the cell's own local-day total from ``wth_base``
      ``chirps_v2``      UCSB-CHG/CHIRPS/DAILY — the long-standing reference product
      ``chirps_v3_rnl``  v3 reanalysis; daily values are pentad totals disaggregated by
                         ERA5, so day-to-day structure is derived rather than observed
      ``chirps_v3_sat``  v3 near-real-time

    ``chirps_mode``:
      ``point``     one 0.05° cell (~5.5 km) containing the coordinate
      ``weighted``  area-weighted mean over the whole 0.25° cell; needs
                    ``chirps_era5_map`` (``uv run python -m src.db.chirps_map``)

    ⚠ **Day definition.** ``wth_base`` days are the cell's LOCAL day; CHIRPS days are the
    product's UTC-anchored day. Mixing them joins two slightly different 24-hour windows.
    Sound at monthly/seasonal totals; a single-day difference between sources is not a
    measurement disagreement, and cannot be corrected after the fact — daily reduction is
    lossy.
    """
    _require_helpers(conn)
    _check_rain(rain)
    _check_mode(chirps_mode)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT date, srad, tmax, tmin, rain, dewp, wind, rhum, rain_coverage,"
            " is_preliminary, imputed FROM wth_body(%s, %s, %s, %s, %s)",
            (lat, lon, year, rain, chirps_mode),
        )
        return [BodyRow(*row) for row in cur.fetchall()]


def body_lines(
    conn,
    lat: float,
    lon: float,
    year: int,
    *,
    rain: str = "era5",
    chirps_mode: str = "point",
) -> list[str]:
    """The formatted ``@DATE ...`` data lines, without the column header."""
    _require_helpers(conn)
    _check_rain(rain)
    _check_mode(chirps_mode)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT wth_body_lines(%s, %s, %s, %s, %s)",
            (lat, lon, year, rain, chirps_mode),
        )
        return [r[0] for r in cur.fetchall()]


# --------------------------------------------------------------------------------
# Whole file
# --------------------------------------------------------------------------------


def wth_lines(
    conn,
    lat: float,
    lon: float,
    year: int,
    *,
    rain: str = "era5",
    chirps_mode: str = "point",
    clim_from: int = 1991,
    clim_to: int = 2020,
) -> list[str]:
    """The complete file, one string per line."""
    _require_helpers(conn)
    _check_rain(rain)
    _check_mode(chirps_mode)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT wth_file(%s, %s, %s, %s, %s, %s, %s)",
            (lat, lon, year, rain, chirps_mode, clim_from, clim_to),
        )
        return [r[0] for r in cur.fetchall()]


def wth_text(conn, lat: float, lon: float, year: int, **kwargs) -> str:
    """The complete file as one string, newline-terminated."""
    return "\n".join(wth_lines(conn, lat, lon, year, **kwargs)) + "\n"


def write_wth(
    conn,
    lat: float,
    lon: float,
    year: int,
    out_dir: str | Path = ".",
    *,
    overwrite: bool = True,
    **kwargs,
) -> Path:
    """Write ``<child_id><year>.WTH`` into ``out_dir`` and return the path.

    The filename is the DSSAT convention: 4-char station code + 4-digit year. The station
    code is the ``child_id``, so the file is self-describing — two sites closer together
    than 0.25° share a cell and therefore a filename, which is the encoding stating its
    resolution rather than a collision to route around.
    """
    lines = wth_lines(conn, lat, lon, year, **kwargs)
    if not lines:
        raise LookupError(f"no data for ({lat}, {lon}) in {year}")
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{locate(lat, lon).child_id}{year}.WTH"
    if path.exists() and not overwrite:
        raise FileExistsError(path)
    path.write_text("\n".join(lines) + "\n")
    return path


# --------------------------------------------------------------------------------
# QA, comparison, regions
# --------------------------------------------------------------------------------


def qa(conn, lat: float, lon: float, year: int) -> dict:
    """Completeness and provenance for one cell-year. Run before writing files.

    ``days_present < days_expected`` with a non-zero ``quarantined`` means the gap is
    explained: the QA node held those cell-days out rather than loading bad values.
    ``era5t_rows`` are preliminary ERA5T and get replaced by the ``update`` DAG — re-export
    after it runs. ``impossible`` must be 0.
    """
    _require_helpers(conn)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM wth_qa(%s, %s, %s)", (lat, lon, year))
        row = cur.fetchone()
    result = dict(row) if row else {}
    for key in ("child_id", "parent_id"):
        if result.get(key):
            result[key] = result[key].strip()
    return result


def missing_days(conn, lat: float, lon: float, year: int) -> list[date]:
    """Days of ``year`` with no row in ``wth_base`` for this cell."""
    _require_helpers(conn)
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM wth_missing_days(%s, %s, %s)", (lat, lon, year))
        return [r[0] for r in cur.fetchall()]


def compare_rain(
    conn,
    lat: float,
    lon: float,
    year: int,
    sources: tuple[str, ...] = ("era5", "chirps_v2", "chirps_v3_rnl"),
) -> list[dict]:
    """Daily rainfall from several sources side by side, plus each source's annual total.

    Use the totals to choose; do not read a single-day difference as a disagreement
    between measurements (see the day-definition note on :func:`body`).
    """
    series = {src: {r.date: r.rain for r in body(conn, lat, lon, year, rain=src)}
              for src in sources}
    dates = sorted({d for s in series.values() for d in s})
    return [{"date": d, **{src: series[src].get(d) for src in sources}} for d in dates]


def cells_in_polygon(conn, wkt: str, *, land_only: bool = True) -> list[Cell]:
    """Every 0.25° cell intersecting a WKT polygon (EPSG:4326).

    The one place ``geom`` and its GiST index earn their keep — point lookups never need
    them. For a real administrative boundary, load the shapefile into a table and join to
    it rather than pasting WKT.
    """
    sql = (
        "SELECT g.child_id, g.parent_id, g.lat, g.lon FROM era5_land_base_grid g "
        "WHERE g.geom && ST_GeomFromText(%s, 4326) "
        "AND ST_Intersects(g.geom, ST_GeomFromText(%s, 4326))"
    )
    if land_only:
        sql += " AND g.is_land"
    sql += " ORDER BY g.child_id"
    with conn.cursor() as cur:
        cur.execute(sql, (wkt, wkt))
        rows = cur.fetchall()
    return [locate(lat, lon) for _child, _parent, lat, lon in rows]


def export_polygon(
    conn,
    wkt: str,
    year: int,
    out_dir: str | Path,
    *,
    land_only: bool = True,
    **kwargs,
) -> list[Path]:
    """One ``.WTH`` per cell intersecting a polygon — the gridded-simulation case.

    Deliberately one file per cell rather than a spatial average: averaging cells turns N
    convective storms into N days of drizzle. Totals survive, daily intensity does not,
    and for rain-fed simulation that systematically changes the water balance. If you do
    want one file for the whole area, average the silver rows first (see
    ``docs/wth_from_sql.md`` §8c) — never average finished ``.WTH`` files.
    """
    written: list[Path] = []
    for cell in cells_in_polygon(conn, wkt, land_only=land_only):
        try:
            written.append(write_wth(conn, cell.lat, cell.lon, year, out_dir, **kwargs))
        except LookupError:
            # A cell inside the polygon but outside the downloaded extent has no rows.
            # Skip it and keep going: a partial export is more useful than an aborted one.
            continue
    return written


# --------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------


def _add_point_args(p: argparse.ArgumentParser, *, year: bool = True) -> None:
    p.add_argument("lat", type=float)
    p.add_argument("lon", type=float)
    if year:
        p.add_argument("year", type=int)


def _add_rain_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--rain", default="era5", choices=RAIN_SOURCES)
    p.add_argument("--chirps-mode", default="point", choices=CHIRPS_MODES)


def _add_clim_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--clim-from", type=int, default=1991)
    p.add_argument("--clim-to", type=int, default=2020)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="wth_export",
        description="Build DSSAT .WTH files from the silver database.",
    )
    ap.add_argument("--host", default=None, help="override POSTGRES_HOST")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("install", help="install sql/wth_helpers.sql (idempotent, read-only)")

    p = sub.add_parser("locate", help="coordinate -> cell codes (no database)")
    _add_point_args(p, year=False)

    p = sub.add_parser("header", help="header fields for a coordinate")
    _add_point_args(p, year=False)
    p.add_argument("--year", type=int, default=None,
                   help="include the *WEATHER DATA description line for this year")
    p.add_argument("--rain", default="era5", choices=RAIN_SOURCES,
                   help="named in the description line only")
    _add_clim_args(p)

    p = sub.add_parser("body", help="daily rows for a cell-year")
    _add_point_args(p)
    _add_rain_args(p)
    p.add_argument("--limit", type=int, default=None)

    p = sub.add_parser("file", help="the complete .WTH file")
    _add_point_args(p)
    _add_rain_args(p)
    _add_clim_args(p)
    p.add_argument("--out", default=None, help="directory to write into (default: stdout)")

    p = sub.add_parser("qa", help="completeness and provenance for a cell-year")
    _add_point_args(p)

    p = sub.add_parser("compare", help="rainfall sources side by side")
    _add_point_args(p)

    p = sub.add_parser("polygon", help="one .WTH per cell inside a WKT polygon")
    p.add_argument("wkt")
    p.add_argument("year", type=int)
    _add_rain_args(p)
    p.add_argument("--out", required=True)
    p.add_argument("--all-cells", action="store_true", help="include non-land cells")

    args = ap.parse_args(argv)

    if args.cmd == "locate":  # the only command that needs no database
        c = locate(args.lat, args.lon)
        print(f"child_id   {c.child_id}    (DSSAT station code)")
        print(f"parent_id  {c.parent_id}    (LIST partition key)")
        print(f"fine_id    {c.fine_id}   (0.05 deg CHIRPS)")
        print(f"fparent_id {c.fparent_id}")
        print(f"cell centre {c.lat:.3f}, {c.lon:.3f}")
        return 0

    with connect(args.host) as conn:
        if args.cmd == "install":
            install_helpers(conn)
            missing = helpers_installed(conn)
            print(f"installed {HELPERS_SQL}")
            print("missing after install:", missing or "none")
            return 1 if missing else 0

        if args.cmd == "header":
            h = header(conn, args.lat, args.lon,
                       clim_from=args.clim_from, clim_to=args.clim_to)
            lines = header_lines(conn, args.lat, args.lon, args.year or 0,
                                 rain=args.rain,
                                 clim_from=args.clim_from, clim_to=args.clim_to)
            # Without a --year there is no meaningful description line, so print only the
            # station block (the two lines DSSAT actually reads by column).
            for line in (lines if args.year else lines[2:]):
                print(line)
            print()
            print(f"# cell {h.child_id} (parent {h.parent_id}) centre {h.lat}, {h.lon}")
            print(f"# t_zone {h.t_zone} min, climatology from {h.n_months} months")
            return 0

        if args.cmd == "body":
            rows = body(conn, args.lat, args.lon, args.year,
                        rain=args.rain, chirps_mode=args.chirps_mode)
            lines = body_lines(conn, args.lat, args.lon, args.year,
                               rain=args.rain, chirps_mode=args.chirps_mode)
            for line in lines[: args.limit]:
                print(line)
            sys.stdout.flush()   # keep the stderr summary below the rows, not above them
            prelim = sum(1 for r in rows if r.is_preliminary)
            repaired = sum(1 for r in rows if r.imputed)
            print(f"# {len(rows)} days, {prelim} preliminary (ERA5T), {repaired} repaired",
                  file=sys.stderr)
            return 0

        if args.cmd == "file":
            if args.out:
                path = write_wth(conn, args.lat, args.lon, args.year, args.out,
                                 rain=args.rain, chirps_mode=args.chirps_mode,
                                 clim_from=args.clim_from, clim_to=args.clim_to)
                print(path)
            else:
                sys.stdout.write(
                    wth_text(conn, args.lat, args.lon, args.year,
                             rain=args.rain, chirps_mode=args.chirps_mode,
                             clim_from=args.clim_from, clim_to=args.clim_to)
                )
            return 0

        if args.cmd == "qa":
            report = qa(conn, args.lat, args.lon, args.year)
            width = max(len(k) for k in report)
            for key, value in report.items():
                print(f"{key:<{width}}  {value}")
            gaps = missing_days(conn, args.lat, args.lon, args.year)
            if gaps:
                shown = ", ".join(str(d) for d in gaps[:10])
                more = f" (+{len(gaps) - 10} more)" if len(gaps) > 10 else ""
                print(f"missing_days  {shown}{more}")
            return 0

        if args.cmd == "compare":
            rows = compare_rain(conn, args.lat, args.lon, args.year)
            if not rows:
                print("no rows")
                return 0
            sources = [k for k in rows[0] if k != "date"]
            print(f"{'date':<12}" + "".join(f"{s:>16}" for s in sources))
            for row in rows:
                cells = "".join(
                    f"{(row[s] if row[s] is not None else float('nan')):>16.2f}"
                    for s in sources
                )
                print(f"{str(row['date']):<12}{cells}")
            totals = {
                s: sum(r[s] for r in rows if r[s] is not None) for s in sources
            }
            print(f"{'total mm':<12}" + "".join(f"{totals[s]:>16.1f}" for s in sources))
            return 0

        if args.cmd == "polygon":
            paths = export_polygon(conn, args.wkt, args.year, args.out,
                                   land_only=not args.all_cells,
                                   rain=args.rain, chirps_mode=args.chirps_mode)
            for path in paths:
                print(path)
            print(f"# {len(paths)} files", file=sys.stderr)
            return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
