"""Silver QA / validation node (PLANNING.md §8.4).

Two separate jobs, deliberately not conflated:

- ``split_valid`` — **row-level physical checks**. Failing rows are quarantined in
  ``wth_qa_failures`` rather than propagated into ``wth_base``, so a query against silver
  never returns a physically impossible value.
- ``calendar_report`` — **coverage reporting**. Missing days are a completeness signal,
  not a property of any row, so they are logged for the operator rather than blocking a
  load (a partial current year is normal — bronze for 2026 ends mid-year).

NaN never fails a check. A NULL derived value is the documented outcome of a missing
input (§8.3); treating it as a QA failure would quarantine perfectly good observations.
"""

from __future__ import annotations

import calendar

import pandas as pd

# (reason, predicate) — predicate returns True for the rows that FAIL. Comparisons
# against NaN are False, so missing values pass through untouched (see module docstring).
CHECKS: list[tuple[str, callable]] = [
    ("tmax<tmin", lambda d: d["tmax"] < d["tmin"]),
    ("precip<0", lambda d: d["precip"] < 0),
    ("srad<0", lambda d: d["srad"] < 0),
    ("rh_out_of_range", lambda d: (d["rh"] < 0) | (d["rh"] > 100)),
    ("et0<0", lambda d: d["et0"] < 0),
]


def split_valid(wide: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split a wide frame into ``(good, failures)`` per §8.4.

    ``failures`` is ``wide`` plus a ``reason`` column; a row failing several checks
    carries all of them, semicolon-joined, so the quarantine table explains itself.
    """
    reasons = pd.Series("", index=wide.index, dtype="object")
    for reason, predicate in CHECKS:
        hit = predicate(wide).fillna(False)
        reasons = reasons.mask(hit, reasons.where(reasons == "", reasons + ";") + reason)

    failed = reasons != ""
    good = wide.loc[~failed].reset_index(drop=True)
    failures = wide.loc[failed].assign(reason=reasons[failed]).reset_index(drop=True)
    return good, failures


def calendar_report(wide: pd.DataFrame, year: int) -> pd.DataFrame:
    """Per-cell day counts for ``year`` vs. the calendar (leap day included).

    Returns one row per ``child_id`` with ``days``, ``expected`` and ``missing``. A
    partial trailing year legitimately shows a shortfall; a *mid-series* shortfall means
    the bronze download lost a chunk.
    """
    expected = 366 if calendar.isleap(year) else 365
    counts = (
        wide.groupby("child_id")["date"]
        .nunique()
        .rename("days")
        .reset_index()
        .assign(expected=expected)
    )
    counts["missing"] = counts["expected"] - counts["days"]
    return counts
