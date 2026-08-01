"""DAG 2b — download_bronze_gee (GEE backend; mirrors download_bronze).

Backfills the bronze layer from **Google Earth Engine** instead of the CDS: Parquet per
variable per year over ``extent``, with hourly→daily reduction done server-side and the
daily result exported via GCS (PLANNING.md §7; docs/step3b_bronze_download_gee.md). Output
schema and paths are identical to the CDS DAG, so the two backends are interchangeable.

**Exports are chunked spatially.** One export of a whole year over a continental extent does
not complete — EE restarts the task (``attempt`` 2, 3) until something cancels it, measured
twice over Brazil. ``chunk_parents`` splits the extent into parent-aligned blocks, one export
each (docs/cost_model_climate_context.md §9.3–9.4); the default 400 (a 20°×20° block) sits
4.9× under the measured ``land_cells × zones`` ceiling. ``chunk_parents=0`` restores the old
whole-extent behaviour, which is still right for a country-sized extent.

Chunk boxes are aligned to the canonical grid, **not clipped to** ``extent`` — that is what
keeps ``chunk_id`` a pure function of position and lets the manifest skip a chunk on a later,
differently-shaped run. A chunked backfill therefore covers a little more ground than the
extent asked for, and those extra land cells reach silver. Harmless (grid-aligned, land-only,
upserted like any other) but not a bug when you see it.

Tasks are mapped one per ``(year, variable, chunk)`` and bound to the ``gee_pool`` Airflow
pool, which caps simultaneous EECU/export tasks (stay under the noncommercial monthly EECU
quota). Per-chunk mapping is deliberate: it is what gives per-chunk retry and lets the pool
hold four *independent* exports, worth a net 2.64× over serial. Create the pool once:
``airflow pools set gee_pool 4 "GEE export cap"``.
"""

from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

GEE_POOL = "gee_pool"

# Airflow refuses to expand a mapped task beyond core.max_map_length (default 1024). A full
# Brazil backfill is 77 years x 7 variables x 8 chunks = 4,312, so the trigger has to be
# windowed. Read the live value rather than assuming the default.
_MAP_LIMIT_FALLBACK = 1024


def _max_map_length() -> int:
    from airflow.configuration import conf

    return conf.getint("core", "max_map_length", fallback=_MAP_LIMIT_FALLBACK)


def _plan(**context) -> list[dict]:
    """Per-task op_kwargs, one per ``(year, variable, chunk)``, in canonical order.

    Chunking is planned **once** here, from the grid alone: ``plan_chunks`` drops ocean-only
    blocks (each would otherwise cost a full EE task for a GeoTIFF with no valid pixel) and
    flags any block whose ``land_cells × zones`` exceeds the measured ceiling. Both terms come
    from ``era5_land_base_grid``, so a doomed export is refused before EE is ever called.
    """
    params = context["params"]
    extent = [float(v) for v in params["extent"]]
    chunk_days = int(params["chunk_days"])
    land_only = bool(params["land_only"])
    data_root = params.get("data_root") or None
    sample = params.get("sample") or None
    max_attempts = int(params.get("max_attempts") or 0) or None
    chunk_parents = int(params.get("chunk_parents") or 0)
    years = list(range(int(params["start_year"]), int(params["end_year"]) + 1))
    variables = list(params["variables"])

    common = {
        "chunk_days": chunk_days,
        "land_only": land_only,
        "data_root": data_root,
        "sample": sample,
        "max_attempts": max_attempts,
    }

    if chunk_parents <= 0:
        spatial = [{"extent": extent}]
    else:
        spatial = _spatial_plan(
            extent, chunk_parents, skip_over_ceiling=bool(params.get("skip_over_ceiling", True))
        )

    tasks = [
        {"variable": variable, "year": year, **part, **common}
        for year in years
        for variable in variables
        for part in spatial
    ]

    limit = _max_map_length()
    if len(tasks) > limit:
        per_year = len(variables) * len(spatial)
        raise ValueError(
            f"plan expands to {len(tasks)} mapped tasks, over Airflow's "
            f"core.max_map_length={limit}. This run is {len(years)} years x "
            f"{len(variables)} variables x {len(spatial)} chunks. Trigger it in year "
            f"windows of at most {limit // per_year} years — which is also how the EECU "
            f"quota gets spread across months (docs/runbook.md, chunked backfill)."
        )
    return tasks


def _spatial_plan(
    extent: list[float], chunk_parents: int, *, skip_over_ceiling: bool
) -> list[dict]:
    """Land-bearing chunks of ``extent`` as op_kwargs fragments.

    ``parent_ids`` is deliberately **not** carried: 400 codes per chunk across thousands of
    mapped tasks is a lot of XCom for something ``_download`` can regenerate exactly from the
    chunk's own box, which is canonical.
    """
    from src.gee.chunks import CELL_ZONE_CEILING, plan_chunks, side_of

    planned = plan_chunks(extent, side_of(chunk_parents))
    if not planned:
        raise ValueError(f"extent {extent} holds no land cells — nothing to export")

    over = [p for p in planned if p.over_ceiling]
    if over and skip_over_ceiling:
        worst = max(over, key=lambda p: p.cell_zones)
        # Halving the side quarters the cells, which is the coarse lever that actually
        # moves a chunk under the line; shaving one parent off the side does not.
        smaller = max(side_of(chunk_parents) // 2, 1) ** 2
        raise ValueError(
            f"{len(over)} of {len(planned)} chunks exceed the measured cell_zones ceiling "
            f"{CELL_ZONE_CEILING} (worst: {worst.chunk_id} at {worst.cell_zones} = "
            f"{worst.land_cells} cells x {worst.zones} zones). EE would restart those "
            f"exports rather than run them. Retry with chunk_parents={smaller} instead of "
            f"{chunk_parents} for this extent — not a higher max_attempts."
        )

    return [
        {
            "extent": list(p.chunk.extent),
            "chunk_id": p.chunk_id,
            "chunk_parents": chunk_parents,
            "land_parents": p.land_parents,
        }
        for p in planned
    ]


def _rebuild_chunk(extent: list, chunk_id: str, chunk_parents: int):
    """The :class:`~src.gee.chunks.Chunk` for a box the plan already decided on.

    Chunks are canonical, so re-tiling a chunk's own box yields that chunk and nothing
    else — parent codes included. Regenerating beats shipping 400 parent codes per task
    through XCom, and the id assertion turns a grid/params mismatch into a loud failure
    instead of an export of the wrong box.
    """
    from src.gee.chunks import side_of, tile_extent

    chunks = tile_extent(list(extent), side_of(int(chunk_parents)))
    if len(chunks) != 1 or chunks[0].chunk_id != chunk_id:
        raise RuntimeError(
            f"chunk {chunk_id} did not round-trip through its own box {extent}: got "
            f"{[c.chunk_id for c in chunks]}. The plan and this worker disagree on the grid."
        )
    return chunks[0]


def _download(
    variable: str,
    year: int,
    extent: list,
    chunk_days: int,
    land_only: bool,
    data_root: str | None = None,
    sample: str | None = None,
    chunk_id: str | None = None,
    chunk_parents: int = 0,
    land_parents: int | None = None,
    max_attempts: int | None = None,
) -> dict:
    # No timezone param: the per-cell local-day offset comes from the tz-polygon asset,
    # derived per UTC offset present in the extent (src/gee/export.tz_zones). §5.3.
    import logging

    from src.config import load_config, resolve_bronze_dir
    from src.gee.client import GEEClient
    from src.gee.download import download_variable_year

    # Manifest is shared with the CDS backend: a (variable, year) done by either is done.
    from src.cds.manifest import Manifest

    log = logging.getLogger(__name__)

    tz_asset = load_config().gee.tz_asset
    if not tz_asset:
        raise RuntimeError("GEE_TZ_ASSET is unset — see scripts/build_tz_asset.py")

    # An export polls every 20s for up to 6 h and otherwise logs nothing, so a slow chunk
    # is indistinguishable from a hung one. Log transitions only — state changes and EE
    # restarts — not the ~1,000 polls a long export makes.
    seen: dict = {}

    def _log_poll(status: dict) -> None:
        from src.gee.export import task_attempt

        key = (status.get("state"), task_attempt(status))
        if key == seen.get("last"):
            return
        seen["last"] = key
        log.info("%s %s chunk %s: export state=%s attempt=%s", variable, year,
                 chunk_id or "-", key[0], key[1])

    chunk = _rebuild_chunk(extent, chunk_id, chunk_parents) if chunk_id else None
    bronze_dir = resolve_bronze_dir(data_root)
    manifest = Manifest.for_bronze_dir(bronze_dir)
    rec: dict = {}
    try:
        path = download_variable_year(
            GEEClient(),
            variable,
            year,
            extent,
            tz_asset=tz_asset,
            manifest=manifest,
            bronze_dir=bronze_dir,
            chunk_days=int(chunk_days),
            land_only=bool(land_only),
            sample=sample,
            metrics_out=rec,
            chunk=chunk,
            land_parents=land_parents,
            max_attempts=max_attempts,
            on_poll=_log_poll,
        )
    except Exception:
        # A chunk that burned through the attempt cap is the signal that this extent's
        # zone count has outgrown the chosen chunk size — it must not be lost in a stack
        # trace among thousands of mapped tasks.
        if rec.get("attempts") and max_attempts and rec["attempts"] > max_attempts:
            log.error(
                "%s %s chunk %s ABORTED at attempt %s (cap %s), %s zones: EE kept "
                "restarting this export. Drop chunk_parents for this extent — do not "
                "raise max_attempts.",
                variable, year, chunk_id, rec.get("attempts"), max_attempts,
                rec.get("n_zones"),
            )
        raise
    # Cost accounting (docs/cost_model_climate_context.md §3). `rec` is empty on a
    # manifest hit — nothing ran, so nothing was measured. eecu_hours may legitimately be
    # None: EE does not always report it, and the task_id in the JSONL is the handle for
    # reading it off the EE task list by hand.
    log.info(
        "%s %s chunk %s: %s rows, %s cells x %s days, %s zones, attempt %s, %s EECU-h, "
        "%s bytes in %s blob(s), export %ss / download %ss / encode %ss",
        variable, year, chunk_id or "-",
        rec.get("bronze_rows"), rec.get("cells"), rec.get("days"), rec.get("n_zones"),
        rec.get("attempts"),
        rec.get("eecu_hours"), rec.get("bytes_remote"), rec.get("n_blobs"),
        rec.get("t_export_s"), rec.get("t_download_s"), rec.get("t_encode_s"),
    )
    if rec.get("record_write_error"):
        log.warning("cost record not persisted: %s", rec["record_write_error"])
    return {
        "variable": variable,
        "year": year,
        "chunk_id": chunk_id,
        "path": str(path),
        "sample": sample or "",
        "bronze_rows": rec.get("bronze_rows"),
        "cells": rec.get("cells"),
        "days": rec.get("days"),
        "n_units": rec.get("n_units"),
        "n_zones": rec.get("n_zones"),
        "attempts": rec.get("attempts"),
        "eecu_hours": rec.get("eecu_hours"),
        "bytes_remote": rec.get("bytes_remote"),
        "n_blobs": rec.get("n_blobs"),
        "compression_ratio": rec.get("compression_ratio"),
        "t_export_s": rec.get("t_export_s"),
        "t_download_s": rec.get("t_download_s"),
        "t_encode_s": rec.get("t_encode_s"),
        "task_id": rec.get("task_id"),
    }


def _rollup(**context) -> dict:
    """Mark a ``(variable, year)`` done once every chunk the plan listed is marked.

    ``mark_spatial_chunk_done`` deliberately only ever records the part: a chunk failing
    must not make its year look complete. Something still has to close the year, though —
    ``available_variables`` and the runbook both talk in var-years, and the CDS backend's
    manifest keyspace is the same one.

    Runs on ``all_done``, so an interrupted backfill still records what landed and a
    re-trigger re-exports only the gaps. Single-threaded on purpose: ``Manifest`` rewrites
    the whole JSON on every mark.
    """
    import logging

    from src.cds.manifest import Manifest
    from src.config import resolve_bronze_dir

    log = logging.getLogger(__name__)
    tasks = context["ti"].xcom_pull(task_ids="plan") or []
    if not tasks:
        return {"marked": 0}

    bronze_dir = resolve_bronze_dir(tasks[0].get("data_root"))
    manifest = Manifest.for_bronze_dir(bronze_dir)

    wanted: dict[tuple[str, int], set[str]] = {}
    for task in tasks:
        if task.get("chunk_id"):
            wanted.setdefault((task["variable"], task["year"]), set()).add(task["chunk_id"])

    marked = incomplete = 0
    for (variable, year), chunk_ids in sorted(wanted.items()):
        missing = [
            cid for cid in sorted(chunk_ids)
            if not manifest.is_spatial_chunk_done(variable, year, cid)
        ]
        if missing:
            incomplete += 1
            log.warning(
                "%s %s incomplete: %d of %d chunks missing (%s%s)",
                variable, year, len(missing), len(chunk_ids), ", ".join(missing[:5]),
                ", …" if len(missing) > 5 else "",
            )
            continue
        if not manifest.is_var_year_done(variable, year):
            manifest.mark_var_year_done(variable, year)
            marked += 1

    log.info("rollup: %d var-years closed, %d still incomplete", marked, incomplete)
    return {"marked": marked, "incomplete": incomplete}


with DAG(
    dag_id="download_bronze_gee",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["era5", "bronze", "gee"],
    # Native rendering so `conf="{{ dag_run.conf }}"` on the transform trigger resolves to the
    # actual dict (variables stays a list, years stay ints) instead of a stringified repr.
    render_template_as_native_obj=True,
    params={
        "extent": [-90.0, -180.0, 90.0, 180.0],  # [S, W, N, E], snapped to 0.25°
        "start_year": 1995,
        "end_year": 1995,
        "variables": ["tmax", "tmin", "precip", "srad", "wind_u", "wind_v", "tdew"],
        "chunk_days": 30,   # daily bands held in RAM per encode step (Pi memory lever)
        "land_only": True,  # clip export to land (LSIB); drop masked-ocean cells
        "chunk_parents": 400,  # parents per export chunk (400 = a 20°x20° block at b=4).
                               # 0 exports the whole extent in one task — right for a
                               # country, fatal for a continent (§9.3). Must be a square.
        "max_attempts": 2,     # EE restarts tolerated before the export is abandoned. A
                               # restart means the export is too big for one task; waiting
                               # out a third only spends more quota on the same failure.
        "skip_over_ceiling": True,  # fail the plan rather than submit an export the grid
                                    # already predicts EE will restart
        "data_root": "",    # per-run data root override; blank = env DATA_DIR. Give each
                            # region its own root (e.g. /data/hn) to avoid manifest clashes.
        "sample": "",       # calibration sample id (e.g. "E1") stamped on each cost record
                            # in <bronze>/_gee_metrics.jsonl. Blank for ordinary backfills.
    },
) as dag:
    plan = PythonOperator(
        task_id="plan",
        python_callable=_plan,
    )
    download = PythonOperator.partial(
        task_id="download",
        python_callable=_download,
        pool=GEE_POOL,
    ).expand(op_kwargs=plan.output)

    # Closes the var-years whose chunks all landed. all_done, not all_success: an
    # interrupted backfill should still record what completed, so a re-trigger re-exports
    # only the gaps. Writes the manifest and nothing else, so it is independent of the
    # transform below rather than upstream of it.
    rollup = PythonOperator(
        task_id="rollup",
        python_callable=_rollup,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # After every (year, variable) download lands, kick transform_silver with the *same* conf
    # (extent/years/variables/data_root) so bronze -> silver runs end-to-end in one trigger.
    # transform_silver reads bronze from the same data_root and ignores the extra download-only
    # keys (extent/chunk_days/land_only); params it doesn't receive keep their own defaults.
    # It globs <var>_<year>*.parquet, so chunk files are picked up like any other bronze.
    kick_transform = TriggerDagRunOperator(
        task_id="kick_transform",
        trigger_dag_id="transform_silver",
        conf="{{ dag_run.conf }}",
        reset_dag_run=True,       # re-runnable: replace a same-logical-date run instead of erroring
        wait_for_completion=False,
    )

    plan >> download >> [rollup, kick_transform]
