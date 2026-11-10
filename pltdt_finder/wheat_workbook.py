"""Wheat sowing-date estimates for several harvest years, written to one Excel workbook.

For every harvest year, the fields whose rotation attributes say winter wheat (``WW``,
see ``io.crop_history``) are run through ``plot_season.run_season`` with the thermal-time
lag. Writes, into ``--out``:

- ``<xlsx>`` with tabs *Wheat presence*, *Sowing estimates*, *Summary*, *Method*. An
  existing workbook keeps its other sheets; these four are replaced.
- per harvest year, the overview / by-field PNGs and the CSV of ``plot_season``.

openpyxl and matplotlib are not project dependencies, so run with::

    POSTGRES_HOST=localhost uv run --with matplotlib,openpyxl \\
        python -m pltdt_finder.wheat_workbook --field-set Weizen_LG_E2026 \\
        --seasons 2024 2025 2026 --out ~/code/mr/Weizen \\
        --reported 2026 2025-09-15 2025-10-15
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

from pltdt_finder import plot_season as ps
from pltdt_finder.io import crop_history

WHEAT = "WW"
SHEETS = ("Wheat presence", "Sowing estimates", "Summary", "Method")
HEAD = Font(bold=True)
YES = PatternFill("solid", fgColor="D9EAD3")
NO = PatternFill("solid", fgColor="F3F3F3")
LOW = PatternFill("solid", fgColor="FCE5CD")
DATE_FMT = "yyyy-mm-dd"

CROP_NAMES = {
    "WW": "winter wheat", "WRA": "winter rapeseed", "WG": "winter barley",
    "MAIS": "maize", "ZR": "sugar beet", "LU": "lucerne", "DI": "DI (unconfirmed)",
}


def weighted_median(dates: pd.Series, weights: pd.Series) -> pd.Timestamp | None:
    d = pd.DataFrame({"d": dates, "w": weights}).dropna().sort_values("d")
    if d.empty:
        return None
    cum = d["w"].cumsum()
    return d.loc[cum >= cum.iloc[-1] / 2, "d"].iloc[0]


def presence_table(hist: pd.DataFrame, seasons: list[int]) -> pd.DataFrame:
    piv = hist.pivot(index=["field_id", "src_id", "name", "ha"], columns="harvest",
                     values="crop").reset_index()
    out = piv[["src_id", "name", "ha"]].copy()
    out.columns = ["Field ID", "Name", "Area (ha)"]
    for y in seasons:
        out[f"Crop {y}"] = piv[y] if y in piv else None
    for y in seasons:
        out[f"Wheat {y}"] = out[f"Crop {y}"].eq(WHEAT).map({True: "yes", False: "no"})
    return out.sort_values("Field ID", key=lambda s: s.astype(int)).reset_index(drop=True)


def estimates_rows(field_set, seasons, hist, reported, gdd_min, gdd_typ, out_dir: Path):
    rows = []
    for season in seasons:
        wheat = hist[(hist.harvest == season) & (hist.crop == WHEAT)]
        if wheat.empty:
            print(f"harvest {season}: no wheat fields")
            continue
        fields = list(wheat["src_id"])
        df, est, labels = ps.run_season(field_set, season, fields, gdd=True,
                                        gdd_min=gdd_min, gdd_typ=gdd_typ)
        rep = reported.get(season)
        print(ps.overview(df, est, labels, season, rep, out_dir, gdd=True))
        print(ps.by_field(df, est, labels, season, rep, out_dir))
        table = ps.estimates_table(est, labels, rep)
        table.to_csv(out_dir / f"sowing_estimates_H{season}.csv", index=False,
                     date_format="%Y-%m-%d")
        prev = ps._prev_crops(field_set, season)
        meta = wheat.set_index("field_id")
        for e in est:
            m = meta.loc[e.field_id]
            rows.append({
                "Harvest": season,
                "Field ID": m["src_id"],
                "Name": m["name"],
                "Area (ha)": m["ha"],
                "Previous crop(s)": prev.get(e.field_id, ""),
                "Cover removed": e.removed,
                "Bare-soil NDVI": None if e.bare_ndvi is None else round(e.bare_ndvi, 3),
                "Last bare date": e.last_bare,
                "Green-up detected": e.greenup,
                "Window start": e.window_start,
                "Window end": e.window_end,
                "Sowing estimate (GDD)": e.sowing,
                "Sowing (fixed 25 d)": e.sowing_fixed,
                "Lag green-up − sowing (d)": e.lag_days,
                "Confident": "yes" if e.confident else "no",
                "In client window": (
                    ("yes" if rep[0] <= e.sowing <= rep[1] else "no")
                    if rep and e.sowing is not None else ""
                ),
                "Days last bare → green-up": (
                    (e.greenup - e.last_bare).days if e.greenup is not None else None
                ),
            })
    return pd.DataFrame(rows)


def summary_table(est: pd.DataFrame) -> pd.DataFrame:
    out = []
    for season, g in est.groupby("Harvest"):
        ok = g[g["Confident"] == "yes"]
        s = ok["Sowing estimate (GDD)"]
        out.append({
            "Harvest": season,
            "Wheat fields": len(g),
            "Wheat area (ha)": round(g["Area (ha)"].sum(), 1),
            "Confident estimates": len(ok),
            "Earliest sowing": s.min(),
            "Median sowing": s.median().normalize() if len(s) else None,
            "Area-weighted median": weighted_median(s, ok["Area (ha)"]),
            "Latest sowing": s.max(),
            "Earliest window start": ok["Window start"].min(),
            "Latest window end": ok["Window end"].max(),
        })
    return pd.DataFrame(out)


def method_lines(gdd_min, gdd_typ, reported) -> list[str]:
    lines = [
        "Source: Sentinel-2 L2A NDVI per 10 m pixel (Cloud Score+ cs_cdf >= 0.6), field "
        "median per date; dates with < 50 % of a field clear are ignored.",
        "Wheat presence: shapefile attributes. FRUCHTART = crop of harvest ERNTEJAHR; "
        "V1_FRUCHT / V2_FRUCHT read as the crops of the 1 and 2 previous harvests. "
        "This reading is an assumption — please confirm with the client.",
        "Crop codes: " + ", ".join(f"{k} = {v}" for k, v in CROP_NAMES.items()) + ".",
        "Per field: cover removal (last NDVI >= 0.45 followed by bare soil), seedbed "
        "(lowest NDVI after removal), green-up = first date NDVI rises > bare + 0.08 "
        "after the last bare date.",
        f"Sowing estimate: walk back from green-up until {gdd_typ:.0f} °C·d (daily mean "
        "of ERA5 tmax/tmin, base 0 °C) have accumulated. Window end: "
        f"{gdd_min:.0f} °C·d before green-up (emergence). Window start: day after "
        "cover removal.",
        "'Sowing (fixed 25 d)' is the earlier method (green-up − 25 days), kept for "
        "comparison.",
        "Green-up can only be seen on a clear date. 'Days last bare → green-up' is the "
        "span in which the real green-up happened; when it is long (cloud gap), the real "
        "sowing may be earlier than the estimate, down to about the window start.",
        "Confident = no: the estimate falls before 1 Sep, i.e. the series shows no clear "
        "bare-soil phase (e.g. volunteers after rapeseed). Treat these rows as unreliable.",
    ]
    for season, (a, b) in sorted(reported.items()):
        lines.append(f"Client-reported sowing window for harvest {season}: "
                     f"{a:%Y-%m-%d} – {b:%Y-%m-%d}.")
    return lines


def _write_df(ws, df: pd.DataFrame) -> None:
    ws.append(list(df.columns))
    for c in ws[1]:
        c.font = HEAD
        c.alignment = Alignment(wrap_text=True, vertical="top")
    for rec in df.itertuples(index=False):
        ws.append([None if (v is None or (not isinstance(v, str) and pd.isna(v)))
                   else (v.to_pydatetime().date() if isinstance(v, pd.Timestamp) else v)
                   for v in rec])
    for j, col in enumerate(df.columns, 1):
        letter = get_column_letter(j)
        is_date = pd.api.types.is_datetime64_any_dtype(df[col]) or any(
            isinstance(v, pd.Timestamp) for v in df[col])
        width = max([len(str(col))] + [len(str(v)) for v in df[col]]) if not is_date else 11
        ws.column_dimensions[letter].width = min(max(width, 9), 42) + 1
        if is_date:
            for cell in ws[letter][1:]:
                cell.number_format = DATE_FMT
    ws.freeze_panes = "C2"
    ws.auto_filter.ref = ws.dimensions


def write_workbook(path: Path, presence, est, summary, method) -> Path:
    wb = load_workbook(path) if path.exists() else Workbook()
    if not path.exists():
        wb.remove(wb.active)
    for name in SHEETS:
        if name in wb.sheetnames:
            wb.remove(wb[name])

    ws = wb.create_sheet("Wheat presence")
    _write_df(ws, presence)
    for row in ws.iter_rows(min_row=2):
        for c in row:
            if c.value == "yes":
                c.fill = YES
            elif c.value == "no":
                c.fill = NO

    ws = wb.create_sheet("Sowing estimates")
    _write_df(ws, est)
    conf_col = list(est.columns).index("Confident") + 1
    for row in ws.iter_rows(min_row=2):
        if row[conf_col - 1].value == "no":
            for c in row:
                c.fill = LOW

    ws = wb.create_sheet("Summary")
    _write_df(ws, summary)
    ws.freeze_panes = "B2"

    ws = wb.create_sheet("Method")
    ws.column_dimensions["A"].width = 120
    for line in method:
        ws.append([line])
        ws.cell(ws.max_row, 1).alignment = Alignment(wrap_text=True, vertical="top")
    wb.save(path)
    return path


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--field-set", required=True)
    ap.add_argument("--seasons", type=int, nargs="+", default=[2024, 2025, 2026])
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--xlsx", default=None,
                    help="workbook file name in --out (default <field_set>_wheat_sowing.xlsx)")
    ap.add_argument("--reported", nargs=3, action="append", default=[],
                    metavar=("HARVEST", "FROM", "TO"),
                    help="client-reported sowing window for one harvest year; repeatable")
    ap.add_argument("--gdd-min", type=float, default=120.0)
    ap.add_argument("--gdd-typ", type=float, default=250.0)
    a = ap.parse_args()

    reported = {int(h): (pd.Timestamp(f), pd.Timestamp(t)) for h, f, t in a.reported}
    a.out.mkdir(parents=True, exist_ok=True)
    hist = crop_history(a.field_set)
    presence = presence_table(hist, a.seasons)
    est = estimates_rows(a.field_set, a.seasons, hist, reported, a.gdd_min, a.gdd_typ,
                         a.out)
    summary = summary_table(est)
    xlsx = a.out / (a.xlsx or f"{a.field_set}_wheat_sowing.xlsx")
    print(write_workbook(xlsx, presence, est, summary,
                         method_lines(a.gdd_min, a.gdd_typ, reported)))
    with pd.option_context("display.width", 200, "display.max_columns", 20):
        print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
