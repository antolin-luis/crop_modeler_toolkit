"""Extract a daily weather time series for one coordinate.

The general read path: a coordinate in, a table out. No year scoping, no file format
opinions — with no date arguments you get the entire record for that cell. Pick variables,
pick a window, pick where rainfall comes from, and that is the whole API.

Companion to ``sql/climate_series.sql``, which holds the queries. This module calls those
functions rather than re-implementing them, so the DBeaver path and the Python path cannot
drift apart.

Install the SQL functions once (idempotent, read-only, creates no tables)::

    uv run python scripts/climate_series.py --install

From the shell (default output is CSV on stdout)::

    # what record exists at this coordinate, before pulling it
    uv run python scripts/climate_series.py -5.175 -50.725 --info

    # the entire record, every variable
    uv run python scripts/climate_series.py -5.175 -50.725 > series.csv

    # a window, three variables, CHIRPS v2 rainfall
    uv run python scripts/climate_series.py -5.175 -50.725 \
        --start 2000-01-01 --end 2020-12-31 \
        --vars tmax,tmin,precip --rain chirps_v2 --out series.csv

    # monthly aggregate (precip summed, everything else averaged)
    uv run python scripts/climate_series.py -5.175 -50.725 --monthly

    # long format for plotting
    uv run python scripts/climate_series.py -5.175 -50.725 --long --vars tmax,precip

From Python::

    from scripts.climate_series import connect, series, info

    with connect() as conn:
        df = series(conn, -5.175, -50.725)                        # whole record
        df = series(conn, -5.175, -50.725, start="2010-01-01",
                    variables=["tmax", "precip"], rain="chirps_v3_rnl")

Units are silver units, unconverted: ``tmax``/``tmin``/``tdew`` °C, ``precip`` mm/day,
``srad`` MJ/m²/day, ``wind`` m/s **at 10 m**, ``rh`` %, ``et0`` mm/day. For DSSAT wind in
km/day at 2 m multiply by ``0.748 * 86.4``; ``scripts/wth_export.py`` does that and writes
``.WTH`` files, which is a different job from this one.

⚠ **Day definition.** ``era5`` rainfall is on the cell's LOCAL day; CHIRPS rainfall is on
the product's UTC-anchored day. Choosing a CHIRPS source therefore mixes two slightly
different 24-hour windows in one row. That is a real property of the data — sound at
monthly and seasonal totals, not something to read into a single-day difference, and not
correctable after the fact because daily reduction is lossy.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

HELPERS_SQL = Path(__file__).resolve().parents[1] / "sql" / "climate_series.sql"

VARIABLES = ("tmax", "tmin", "precip", "srad", "wind", "tdew", "rh", "et0")
META_COLUMNS = ("rain_coverage", "is_preliminary", "imputed")
RAIN_SOURCES = ("era5", "chirps_v2", "chirps_v3_rnl", "chirps_v3_sat")
CHIRPS_MODES = ("point", "weighted")

REQUIRED_FUNCTIONS = ("climate_series", "climate_series_info", "climate_monthly")


# --------------------------------------------------------------------------------
# Connection
#
# Kept self-contained (rather than imported from scripts/wth_export.py) so this file runs
# standalone: `python scripts/climate_series.py` puts scripts/ on sys.path, not the repo
# root, so a sibling-module import would work when imported and fail when executed.
# --------------------------------------------------------------------------------


def dsn(host: str | None = None) -> str:
    """Build a Postgres DSN from ``.env`` / the environment.

    Host precedence: the ``host`` argument, ``WTH_PG_HOST``, ``POSTGRES_HOST``, then
    ``localhost``. Deliberately not ``src.config.load_config()``: that also requires
    ``CDS_KEY``, which has nothing to do with reading the database.
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
    """Open a psycopg2 connection, falling back to ``localhost`` when the configured host
    is a container name that does not resolve from here.

    ``.env`` legitimately holds ``POSTGRES_HOST=postgres`` for the compose network; without
    this fallback every host-side run would need an env override, and the failure reads
    like a database outage instead of a name-resolution detail.
    """
    try:
        return psycopg2.connect(dsn(host))
    except psycopg2.OperationalError as exc:
        if "could not translate host name" not in str(exc) or host is not None:
            raise
        if not quiet:
            print(
                "note: POSTGRES_HOST is a container name and does not resolve here; "
                "retrying on localhost:5432",
                file=sys.stderr,
            )
        return psycopg2.connect(dsn("localhost"))


def install(conn, script: Path = HELPERS_SQL) -> None:
    """Run ``sql/climate_series.sql``. Idempotent — every function is CREATE OR REPLACE."""
    with conn.cursor() as cur:
        cur.execute(script.read_text())
    conn.commit()


def _require_functions(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT proname FROM pg_proc WHERE proname = ANY(%s)",
            (list(REQUIRED_FUNCTIONS),),
        )
        present = {row[0] for row in cur.fetchall()}
    missing = [f for f in REQUIRED_FUNCTIONS if f not in present]
    if missing:
        raise RuntimeError(
            f"missing SQL functions: {', '.join(missing)}. Install them once with:\n"
            "    uv run python scripts/climate_series.py --install"
        )


# --------------------------------------------------------------------------------
# Validation
# --------------------------------------------------------------------------------


def _check_rain(rain: str) -> str:
    if rain.lower() not in RAIN_SOURCES:
        raise ValueError(f"rain must be one of {RAIN_SOURCES}, got {rain!r}")
    return rain.lower()


def _check_mode(mode: str) -> str:
    if mode.lower() not in CHIRPS_MODES:
        raise ValueError(f"chirps_mode must be one of {CHIRPS_MODES}, got {mode!r}")
    return mode.lower()


def _check_variables(variables) -> list[str]:
    """Validate against the known set.

    These names are interpolated into the SELECT list (a column list cannot be a bound
    parameter), so the whitelist is what keeps that safe — never relax it to "anything the
    caller passed".
    """
    if variables is None:
        return list(VARIABLES)
    if isinstance(variables, str):
        variables = [v.strip() for v in variables.split(",") if v.strip()]
    unknown = [v for v in variables if v not in VARIABLES + META_COLUMNS]
    if unknown:
        raise ValueError(
            f"unknown variable(s) {', '.join(unknown)}; "
            f"known: {', '.join(VARIABLES)} (plus meta: {', '.join(META_COLUMNS)})"
        )
    return list(variables)


# --------------------------------------------------------------------------------
# Reads
# --------------------------------------------------------------------------------


def info(conn, lat: float, lon: float) -> dict:
    """What record exists at this coordinate: cell identity, extent, gaps, provenance.

    Worth running before a big pull. ``n_days = 0`` means the cell was never loaded —
    outside the downloaded extent, or ocean. ``cell_lat``/``cell_lon`` are the 0.25° cell
    CENTRE, which is where the data actually is; the requested coordinate can be up to
    0.125° away.
    """
    _require_functions(conn)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM climate_series_info(%s, %s)", (lat, lon))
        row = cur.fetchone()
    if row is None:
        raise LookupError(f"no grid cell for ({lat}, {lon}) — outside the seeded grid?")
    result = dict(row)
    for key in ("child_id", "parent_id", "fine_id"):
        if result.get(key):
            result[key] = result[key].strip()
    return result


def series(
    conn,
    lat: float,
    lon: float,
    start: str | date | None = None,
    end: str | date | None = None,
    *,
    variables: list[str] | str | None = None,
    rain: str = "era5",
    chirps_mode: str = "point",
    meta: bool = False,
) -> pd.DataFrame:
    """Daily series for the cell containing ``(lat, lon)``, indexed by date.

    ``start`` / ``end`` are both optional and independent: omit both for the entire
    record, omit one for an open-ended window.

    ``variables`` defaults to all eight. ``meta=True`` adds ``rain_coverage``,
    ``is_preliminary`` (ERA5T rather than final ERA5) and ``imputed`` (a repair bitmask:
    ``tmax=1 tmin=2 precip=4 srad=8 wind=16 tdew=32 rh=64 et0=128``).

    ``rain`` selects the precipitation source; everything else always comes from ERA5.
    ``chirps_mode='weighted'`` area-averages the fine cells over the whole 0.25° cell and
    needs ``chirps_era5_map`` (``uv run python -m src.db.chirps_map``); ``'point'`` uses
    the single 0.05° cell containing the coordinate.

    Empty frame if the cell was never loaded — check :func:`info` first.
    """
    _require_functions(conn)
    rain = _check_rain(rain)
    chirps_mode = _check_mode(chirps_mode)
    cols = _check_variables(variables)
    if meta:
        cols += [c for c in META_COLUMNS if c not in cols]

    # The column list is validated against a whitelist above; the arguments are bound.
    sql = (
        f"SELECT date, {', '.join(cols)} "
        "FROM climate_series(%s, %s, %s, %s, %s, %s)"
    )
    with conn.cursor() as cur:
        cur.execute(sql, (lat, lon, start, end, rain, chirps_mode))
        rows = cur.fetchall()

    frame = pd.DataFrame(rows, columns=["date", *cols])
    return frame.set_index("date")


def series_long(
    conn,
    lat: float,
    lon: float,
    start: str | date | None = None,
    end: str | date | None = None,
    *,
    variables: list[str] | str | None = None,
    rain: str = "era5",
    chirps_mode: str = "point",
) -> pd.DataFrame:
    """The same data as ``(date, variable, value)`` — for plotting and faceting."""
    _require_functions(conn)
    rain = _check_rain(rain)
    chirps_mode = _check_mode(chirps_mode)
    wanted = [v for v in _check_variables(variables) if v in VARIABLES]
    with conn.cursor() as cur:
        cur.execute(
            "SELECT date, variable, value "
            "FROM climate_series_long(%s, %s, %s, %s, %s, %s, %s)",
            (lat, lon, start, end, rain, wanted, chirps_mode),
        )
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=["date", "variable", "value"])


def monthly(
    conn,
    lat: float,
    lon: float,
    start: str | date | None = None,
    end: str | date | None = None,
    *,
    rain: str = "era5",
    chirps_mode: str = "point",
) -> pd.DataFrame:
    """Monthly aggregate: precipitation and ET0 summed, everything else averaged.

    Also the right resolution for comparing rainfall sources — coarse enough that the
    local-day vs product-day mismatch stops mattering.
    """
    _require_functions(conn)
    rain = _check_rain(rain)
    chirps_mode = _check_mode(chirps_mode)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT * FROM climate_monthly(%s, %s, %s, %s, %s, %s)",
            (lat, lon, start, end, rain, chirps_mode),
        )
        rows = [dict(r) for r in cur.fetchall()]
    return pd.DataFrame(rows).set_index("month") if rows else pd.DataFrame()


def missing_days(
    conn,
    lat: float,
    lon: float,
    start: str | date | None = None,
    end: str | date | None = None,
) -> list[date]:
    """Days with no row, inside the record (or inside an explicit window)."""
    _require_functions(conn)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM climate_series_missing(%s, %s, %s, %s)", (lat, lon, start, end)
        )
        return [r[0] for r in cur.fetchall()]


def compare_rain(
    conn,
    lat: float,
    lon: float,
    start: str | date | None = None,
    end: str | date | None = None,
    sources: tuple[str, ...] = ("era5", "chirps_v2", "chirps_v3_rnl"),
    *,
    freq: str = "monthly",
) -> pd.DataFrame:
    """Rainfall from several sources side by side.

    ``freq='monthly'`` (default) compares monthly totals, which is the resolution at which
    the sources are actually comparable. ``freq='daily'`` gives the daily columns — useful
    for inspection, misleading as evidence that two products disagree.
    """
    out = {}
    for src in sources:
        if freq == "monthly":
            out[src] = monthly(conn, lat, lon, start, end, rain=src)["precip_sum"]
        else:
            out[src] = series(conn, lat, lon, start, end,
                              variables=["precip"], rain=src)["precip"]
    return pd.DataFrame(out)


# --------------------------------------------------------------------------------
# Output
# --------------------------------------------------------------------------------


def write(frame: pd.DataFrame, out: str | Path | None, fmt: str = "csv") -> None:
    """Write a frame to a path, or to stdout when ``out`` is None."""
    if fmt == "parquet":
        if out is None:
            raise ValueError("parquet needs --out; it cannot be written to stdout")
        frame.to_parquet(out)
        return
    sep = "\t" if fmt == "tsv" else ","
    if fmt == "json":
        text = frame.reset_index().to_json(orient="records", date_format="iso", indent=2)
    else:
        # Long format carries a meaningless RangeIndex; wide/monthly are indexed by date
        # and that index is the first column of the output.
        keep_index = not isinstance(frame.index, pd.RangeIndex)
        text = frame.to_csv(sep=sep, index=keep_index)
    if out is None:
        sys.stdout.write(text if text.endswith("\n") else text + "\n")
    else:
        Path(out).write_text(text)


# --------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="climate_series",
        description="Daily weather time series for one coordinate. "
                    "With no --start/--end you get the entire record.",
    )
    ap.add_argument("lat", type=float, nargs="?")
    ap.add_argument("lon", type=float, nargs="?")
    ap.add_argument("--start", default=None, help="YYYY-MM-DD (default: record start)")
    ap.add_argument("--end", default=None, help="YYYY-MM-DD (default: record end)")
    ap.add_argument("--vars", default=None,
                    help=f"comma-separated subset of: {','.join(VARIABLES)}")
    ap.add_argument("--rain", default="era5", choices=RAIN_SOURCES,
                    help="precipitation source (everything else is always ERA5)")
    ap.add_argument("--chirps-mode", default="point", choices=CHIRPS_MODES)
    ap.add_argument("--meta", action="store_true",
                    help="include rain_coverage, is_preliminary, imputed")
    ap.add_argument("--long", action="store_true", help="long format (date, variable, value)")
    ap.add_argument("--monthly", action="store_true", help="monthly aggregate")
    ap.add_argument("--compare", action="store_true",
                    help="rainfall sources side by side (monthly totals)")
    ap.add_argument("--info", action="store_true",
                    help="describe the record at this coordinate and exit")
    ap.add_argument("--missing", action="store_true", help="list days with no row and exit")
    ap.add_argument("--out", default=None, help="output file (default: stdout)")
    ap.add_argument("--format", default="csv", choices=("csv", "tsv", "json", "parquet"))
    ap.add_argument("--host", default=None, help="override POSTGRES_HOST")
    ap.add_argument("--install", action="store_true",
                    help="install sql/climate_series.sql and exit")
    args = ap.parse_args(argv)

    with connect(args.host) as conn:
        if args.install:
            install(conn)
            print(f"installed {HELPERS_SQL}")
            return 0

        if args.lat is None or args.lon is None:
            ap.error("lat and lon are required (unless --install)")

        if args.info:
            report = info(conn, args.lat, args.lon)
            width = max(len(k) for k in report)
            for key, value in report.items():
                print(f"{key:<{width}}  {value}")
            return 0

        if args.missing:
            gaps = missing_days(conn, args.lat, args.lon, args.start, args.end)
            for day in gaps:
                print(day)
            print(f"# {len(gaps)} missing days", file=sys.stderr)
            return 0

        if args.compare:
            frame = compare_rain(conn, args.lat, args.lon, args.start, args.end)
        elif args.monthly:
            frame = monthly(conn, args.lat, args.lon, args.start, args.end,
                            rain=args.rain, chirps_mode=args.chirps_mode)
        elif args.long:
            frame = series_long(conn, args.lat, args.lon, args.start, args.end,
                                variables=args.vars, rain=args.rain,
                                chirps_mode=args.chirps_mode)
        else:
            frame = series(conn, args.lat, args.lon, args.start, args.end,
                           variables=args.vars, rain=args.rain,
                           chirps_mode=args.chirps_mode, meta=args.meta)

        if frame.empty:
            print(f"no data for ({args.lat}, {args.lon}) — try --info", file=sys.stderr)
            return 1

        write(frame, args.out, args.format)
        if args.out:
            print(f"{args.out}: {len(frame):,} rows", file=sys.stderr)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, LookupError, RuntimeError, psycopg2.Error) as exc:
        # Bad variable names, unknown rain sources and un-built CHIRPS maps are user input
        # errors with a one-line explanation already attached. A traceback here would bury
        # it; the library functions still raise normally for callers that want one.
        # psycopg2 appends a plpgsql CONTEXT traceback; the RAISE message is the first line.
        print(f"error: {str(exc).strip().splitlines()[0]}", file=sys.stderr)
        raise SystemExit(2) from None
