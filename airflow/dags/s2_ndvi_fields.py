"""DAG — s2_ndvi_fields.

Per-pixel Sentinel-2 NDVI time series for every 10 m pixel inside a set of field polygons,
written straight to Postgres (``s2_ndvi_obs``) — there is no bronze layer. Feeds the
planting-date finder in ``pltdt_finder/``.

    ensure_schema -> load_fields -> build_pixels -> plan -> extract (mapped) -> validate

- ``load_fields`` upserts the shapefile's polygons into ``field_parcel`` (one shapefile =
  one ``field_set``).
- ``build_pixels`` lays out ``s2_pixel``: the native S2 10 m cells whose centre lies in a
  field, in the UTM zone of the field's centroid.
- ``plan`` fans out one task per (field, calendar year), skipping windows already done,
  on ``ee_interactive_pool``.
- ``extract`` pulls each window synchronously with ``computePixels`` (no GCS), keeps
  observations with Cloud Score+ ``cs_cdf >= cs_threshold``, and upserts them.

**Prerequisite.** The shapefile must sit under ``$DATA_DIR`` (``.localdata/`` on the
host); nothing else is mounted into these containers. ``.shp``, ``.dbf`` and ``.prj`` in
WGS84 lon/lat.

Rerun freely: done windows are skipped unless ``force``. The current year's window ends
today, so a rerun on a later day re-pulls it and picks up new acquisitions. Example::

    {"shapefile": "fields/weizen/Weizen_LG_E2026.shp", "start_date": "2025-08-01"}
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# Not gee_pool: computePixels uses the interactive quota, not the batch export queue, so
# it must not queue behind an ERA5 backfill. Created by airflow-init (docker-compose.yml).
EE_INTERACTIVE_POOL = "ee_interactive_pool"
_MAP_LIMIT_FALLBACK = 1024


def _field_set(params: dict) -> str:
    return params["field_set"] or Path(params["shapefile"]).stem


def _ensure_schema(params: dict) -> None:
    from src.db import load as db_load
    from src.fields.load import ensure_schema

    conn = db_load.connect()
    try:
        ensure_schema(conn)
    finally:
        conn.close()


def _load_fields(params: dict) -> int:
    from src.config import resolve_data_path
    from src.db import load as db_load
    from src.fields.load import load_fields

    path = resolve_data_path(params["shapefile"])
    conn = db_load.connect()
    try:
        n = load_fields(
            conn,
            path,
            _field_set(params),
            id_column=params["id_column"],
            name_column=params["name_column"] or None,
        )
    finally:
        conn.close()
    print(f"{path.name}: {n} fields upserted into set {_field_set(params)!r}")
    return n


def _build_pixels(params: dict) -> int:
    from src.db import load as db_load
    from src.fields.load import build_pixels

    conn = db_load.connect()
    try:
        n = build_pixels(conn, _field_set(params))
    finally:
        conn.close()
    print(f"set {_field_set(params)!r}: {n:,} S2 pixels")
    return n


def _plan(params: dict) -> list[dict]:
    from airflow.configuration import conf

    from src.db import load as db_load
    from src.fields.extract import fetch_done, plan_windows
    from src.fields.load import field_ids

    start = dt.date.fromisoformat(params["start_date"])
    end = dt.date.fromisoformat(params["end_date"]) if params["end_date"] else dt.date.today()
    cs = float(params["cs_threshold"])
    if not 0.0 <= cs <= 1.0:
        raise ValueError(f"cs_threshold must be in [0, 1], got {cs}")

    conn = db_load.connect()
    try:
        ids = field_ids(conn, _field_set(params))
        done = fetch_done(conn, ids)
    finally:
        conn.close()
    if not ids:
        raise RuntimeError(f"no fields in set {_field_set(params)!r}")

    windows = plan_windows(ids, start, end, cs, done, force=bool(params["force"]))
    limit = conf.getint("core", "max_map_length", fallback=_MAP_LIMIT_FALLBACK)
    if len(windows) > limit:
        raise ValueError(
            f"{len(windows)} field-year windows exceed max_map_length={limit}; "
            "narrow start_date/end_date and trigger in pieces"
        )
    print(f"{len(ids)} fields, {len(windows)} windows to extract ({start}..{end})")
    return windows


def _extract(field_id: int, year: int, start: str, end: str, cs_threshold: float) -> dict:
    from src.db import load as db_load
    from src.fields.extract import extract_field_window
    from src.gee.client import GEEClient

    GEEClient()
    conn = db_load.connect()
    try:
        out = extract_field_window(conn, field_id, year, start, end, cs_threshold)
    finally:
        conn.close()
    print(out)
    return out


def _validate(params: dict) -> list[dict]:
    from src.db import load as db_load
    from src.fields.load import validate

    conn = db_load.connect()
    try:
        report = validate(conn, _field_set(params))
    finally:
        conn.close()
    for row in report:
        print(row)
    return report


with DAG(
    dag_id="s2_ndvi_fields",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["s2", "ndvi", "fields", "gee"],
    render_template_as_native_obj=True,
    params={
        # Path relative to $DATA_DIR (host: .localdata/). .dbf/.prj sit next to it.
        "shapefile": "fields/weizen/Weizen_LG_E2026.shp",
        # Blank = the shapefile's stem. Re-loading the same set upserts by id.
        "field_set": "",
        # DBF column that identifies a field (unique within the file).
        "id_column": "ID",
        # DBF column copied to field_parcel.name; blank or missing = none.
        "name_column": "NAME",
        # S2 L2A starts 2017-03-28 in GEE. Narrow it when the full archive is not needed.
        "start_date": "2017-03-28",
        # Inclusive; blank = today.
        "end_date": "",
        # Cloud Score+ cs_cdf floor (0..1). Stored per obs, so tighten later in SQL.
        "cs_threshold": 0.6,
        # Re-extract windows already marked done.
        "force": False,
    },
) as dag:
    ensure_schema = PythonOperator(task_id="ensure_schema", python_callable=_ensure_schema)
    load_fields = PythonOperator(task_id="load_fields", python_callable=_load_fields)
    build_pixels = PythonOperator(task_id="build_pixels", python_callable=_build_pixels)
    plan = PythonOperator(task_id="plan", python_callable=_plan)
    extract = PythonOperator.partial(
        task_id="extract",
        python_callable=_extract,
        pool=EE_INTERACTIVE_POOL,
    ).expand(op_kwargs=plan.output)
    # none_failed: a rerun with nothing left to extract maps zero tasks (skipped), and the
    # summary is still wanted.
    validate = PythonOperator(
        task_id="validate",
        python_callable=_validate,
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    ensure_schema >> load_fields >> build_pixels >> plan >> extract >> validate
