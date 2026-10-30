"""Plot a field set's NDVI season with the estimated sowing dates (``greenup.py``).

Writes two PNGs and a CSV into ``--out``:

- ``ndvi_sowing_overview.png``: every field's NDVI over the season (median of its clear
  pixels per date) with the farm median, and below it each field's possible sowing window
  and point estimate against the reported window.
- ``ndvi_sowing_by_field.png``: one autumn panel per field, the same estimate drawn on
  its own series.
- ``sowing_estimates.csv``: the estimates.

matplotlib is not a project dependency (the Airflow image does not need it), so run with::

    POSTGRES_HOST=localhost uv run --with matplotlib python -m pltdt_finder.plot_season \\
        --field-set Weizen_LG_E2026 --season 2026 --out /path/to/folder \\
        --reported 2025-09-15 2025-10-15
"""

from __future__ import annotations

import argparse
from dataclasses import asdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

from pltdt_finder.greenup import clear_series, estimate_sowing  # noqa: E402
from pltdt_finder.io import load_field_mean_series  # noqa: E402
from src.db import load as db_load  # noqa: E402

# Reference palette, light mode (validated: blue/orange pass every check).
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_2 = "#52514e"
MUTED = "#a3a29c"
GRID = "#e6e5e1"
BLUE = "#2a78d6"     # NDVI / our estimate
ORANGE = "#eb6834"   # reported (client) window
MIN_CLEAR = 0.5

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": GRID, "axes.labelcolor": INK_2, "xtick.color": INK_2,
    "ytick.color": INK_2, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
    "axes.spines.top": False, "axes.spines.right": False, "font.size": 9,
    "axes.titlesize": 9, "axes.titlecolor": INK,
})


def _prev_crops(field_set: str) -> dict[int, str]:
    conn = db_load.connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT field_id, attrs->>'V1_FRUCHT', attrs->>'V2_FRUCHT' "
                "FROM field_parcel WHERE field_set = %s",
                (field_set,),
            )
            return {r[0]: "/".join(v for v in r[1:] if v) for r in cur.fetchall()}
    finally:
        conn.close()


def _fmt_month(ax):
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))


def overview(df, est, labels, season, reported, out: Path) -> Path:
    s0, s1 = pd.Timestamp(f"{season - 1}-08-01"), pd.Timestamp(f"{season}-07-31")
    fig, (ax, gx) = plt.subplots(
        2, 1, figsize=(11, 8.5), sharex=True,
        gridspec_kw={"height_ratios": [2.1, 2.0], "hspace": 0.08},
    )
    for fid, g in df.groupby("field_id"):
        c = clear_series(g, str(s0.date()), str(s1.date()), MIN_CLEAR)
        ax.plot(c["date"], c["ndvi_median"], color=MUTED, lw=0.8, alpha=0.7)
    clear = df[(df.clear_frac >= MIN_CLEAR) & df.date.between(s0, s1)]
    farm = clear.groupby("date").filter(lambda x: len(x) >= 10).groupby("date")["ndvi_median"]
    farm = farm.median()
    ax.plot(farm.index, farm.values, color=BLUE, lw=2, marker="o", ms=3.5,
            label="Farm median (dates with ≥10 fields clear)")
    ax.plot([], [], color=MUTED, lw=0.8, label="Each field (median of its clear pixels)")

    for a in (ax, gx):
        a.axvspan(*reported, color=ORANGE, alpha=0.14, lw=0)
    ax.text(reported[0] + (reported[1] - reported[0]) / 2, 0.93,
            "Reported sowing\n15 Sep – 15 Oct", ha="center", va="top", color=INK, fontsize=8)
    ax.set_ylim(0, 1)
    ax.set_ylabel("NDVI")
    ax.set_title(f"{labels['set']} — Sentinel-2 NDVI, harvest {season}: "
                 "possible sowing dates", loc="left", fontsize=11)
    # Winter dates where the whole farm reads as bare are snow, not crop loss — say so.
    snow = farm[(farm < 0.15) & farm.index.month.isin([12, 1, 2])]
    for d, v in snow.items():
        ax.annotate("snow", (d, v), xytext=(6, 2), textcoords="offset points",
                    fontsize=7.5, color=INK_2)
    ax.legend(loc="lower right", bbox_to_anchor=(0.86, 0.0), frameon=False, fontsize=8)

    rows = sorted(est, key=lambda e: (e.sowing is None, e.sowing or s1))
    for y, e in enumerate(rows):
        if e.window_start is None:
            continue
        conf = e.confident
        gx.plot([e.window_start, e.window_end], [y, y], color=BLUE, lw=5,
                alpha=0.35 if conf else 0.15, solid_capstyle="round")
        gx.plot([e.sowing], [y], marker="o", ms=6, color=BLUE if conf else SURFACE,
                markeredgecolor=BLUE, markeredgewidth=1.5)
        gx.plot([e.greenup], [y], marker="|", ms=9, color=INK_2, markeredgewidth=1.5)
    gx.set_yticks(range(len(rows)))
    gx.set_yticklabels([labels[e.field_id] + ("" if e.confident else "  (low conf.)")
                        for e in rows], fontsize=7.5)
    gx.invert_yaxis()
    gx.grid(axis="y", visible=False)
    gx.plot([], [], color=BLUE, lw=5, alpha=0.35, label="Possible sowing window")
    gx.plot([], [], "o", color=BLUE, ms=6, label="Point estimate (green-up − 25 d)")
    gx.plot([], [], "|", color=INK_2, ms=9, markeredgewidth=1.5, label="Green-up detected")
    gx.legend(loc="lower right", frameon=False, fontsize=8)
    gx.set_xlim(s0, s1)
    _fmt_month(gx)
    fig.text(0.01, 0.045,
             "Heuristic: sowing after the last cover removal, before NDVI rises above "
             "bare soil for good; dates with <50 % of a field clear are ignored. "
             "Cloud gap 2 Oct – 3 Nov limits precision.", fontsize=7, color=INK_2)
    path = out / "ndvi_sowing_overview.png"
    fig.savefig(path, dpi=160, bbox_inches="tight")
    plt.close(fig)
    return path


def by_field(df, est, labels, season, reported, out: Path) -> Path:
    s0, s1 = pd.Timestamp(f"{season - 1}-08-01"), pd.Timestamp(f"{season - 1}-12-31")
    by_id = {e.field_id: e for e in est}
    ids = sorted(by_id)
    ncol = 4
    nrow = -(-len(ids) // ncol)
    fig, axes = plt.subplots(nrow, ncol, figsize=(13, 2.3 * nrow), sharex=True, sharey=True)
    for ax, fid in zip(axes.flat, ids):
        e = by_id[fid]
        g = df[(df.field_id == fid) & df.date.between(s0, s1)]
        c = g[g.clear_frac >= MIN_CLEAR]
        ax.axvspan(*reported, color=ORANGE, alpha=0.14, lw=0)
        ax.plot(c["date"], c["ndvi_median"], color=BLUE, lw=1.4, marker="o", ms=3)
        if e.window_start is not None:
            ax.axvspan(e.window_start, e.window_end, ymin=0, ymax=0.06, color=BLUE,
                       alpha=0.5 if e.confident else 0.2, lw=0)
            ax.axvline(e.sowing, color=BLUE, lw=1.2, ls="--")
            ax.annotate(e.sowing.strftime("%d %b"), (e.sowing, 0.97), xytext=(3, 0),
                        textcoords="offset points", va="top", fontsize=7.5, color=INK)
        ax.set_title(labels[fid] + ("" if e.confident else "  · low conf."), loc="left",
                     fontsize=8)
        ax.set_ylim(0, 1)
        _fmt_month(ax)
    for ax in list(axes.flat)[len(ids):]:
        ax.set_visible(False)
    fig.suptitle(
        f"{labels['set']} — autumn {season - 1} NDVI per field. Orange: reported sowing "
        "(15 Sep – 15 Oct). Dashed: estimated sowing; bar at the bottom: possible window.",
        x=0.01, ha="left", fontsize=10, color=INK,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.97))
    path = out / "ndvi_sowing_by_field.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--field-set", required=True)
    ap.add_argument("--season", type=int, required=True, help="harvest year, e.g. 2026")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--reported", nargs=2, metavar=("FROM", "TO"),
                    default=["2025-09-15", "2025-10-15"])
    a = ap.parse_args()

    y0 = a.season - 1
    df = load_field_mean_series(a.field_set, start=f"{y0}-07-01", end=f"{a.season}-08-31")
    prev = _prev_crops(a.field_set)
    labels = {"set": a.field_set}
    for (fid, src, name), _ in df.groupby(["field_id", "src_id", "name"]):
        labels[fid] = f"{src} · {name}" + (f"  (prev. {prev[fid]})" if prev.get(fid) else "")

    est = [
        estimate_sowing(g, fid, season_start=f"{y0}-08-01", season_end=f"{y0}-12-31",
                        bare_end=f"{y0}-11-15", earliest=f"{y0}-09-01")
        for fid, g in df.groupby("field_id")
    ]
    reported = tuple(pd.Timestamp(d) for d in a.reported)
    a.out.mkdir(parents=True, exist_ok=True)
    print(overview(df, est, labels, a.season, reported, a.out))
    print(by_field(df, est, labels, a.season, reported, a.out))

    table = pd.DataFrame([asdict(e) for e in est])
    table.insert(1, "field", table["field_id"].map(labels))
    table["in_reported_window"] = table["sowing"].between(*reported)
    path = a.out / "sowing_estimates.csv"
    table.to_csv(path, index=False, date_format="%Y-%m-%d")
    print(path, f"— {int(table['in_reported_window'].sum())}/{len(table)} inside reported")


if __name__ == "__main__":
    main()
