"""Idempotent bronze download manifest (PLANNING.md §11.4).

A JSON file under ``bronze_dir/_manifest.json`` recording what the backfill has already
fetched, so a restarted run skips finished work — essential for a 50-year × ~7-variable
(≥350 request) backfill where transient failures and queue timeouts are certain.

Two granularities are tracked:

- **(variable, year)** — marked done once the per-variable-per-year Parquet is fully
  written; ``is_var_year_done`` lets the DAG skip a completed file outright.
- **time sub-chunks** within a (variable, year) — the accepted ``[start, end]`` date
  ranges the splitter has already downloaded. If a year is split into months and the
  process crashes mid-year, the next run resumes from the last good chunk instead of
  re-downloading the whole year.

Writes are atomic (temp file + ``os.replace``) and a missing or corrupt file is treated
as empty, so a crash mid-write never corrupts the manifest.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

_FILENAME = "_manifest.json"


def _vy_key(variable: str, year: int) -> str:
    return f"{variable}:{year}"


def _chunk_key(start: str, end: str) -> str:
    return f"{start}/{end}"


def _spatial_key(chunk_id: str) -> str:
    return f"sp:{chunk_id}"


class Manifest:
    """On-disk record of completed downloads. Not thread-safe; one writer at a time."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._data = self._read()

    @classmethod
    def for_bronze_dir(cls, bronze_dir: str | Path) -> "Manifest":
        return cls(Path(bronze_dir) / _FILENAME)

    # --- read / write -----------------------------------------------------------
    def _read(self) -> dict:
        try:
            raw = json.loads(self.path.read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            return {"var_years": {}}
        if not isinstance(raw, dict) or "var_years" not in raw:
            return {"var_years": {}}
        return raw

    def _write(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # Temp file in the same dir so os.replace is atomic (same filesystem).
        fd, tmp = tempfile.mkstemp(dir=self.path.parent, suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as fh:
                json.dump(self._data, fh, indent=2, sort_keys=True)
            os.replace(tmp, self.path)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise

    def _entry(self, variable: str, year: int) -> dict:
        return self._data["var_years"].setdefault(
            _vy_key(variable, year), {"done": False, "chunks": []}
        )

    # --- (variable, year) -------------------------------------------------------
    def is_var_year_done(self, variable: str, year: int) -> bool:
        entry = self._data["var_years"].get(_vy_key(variable, year))
        return bool(entry and entry.get("done"))

    def mark_var_year_done(self, variable: str, year: int) -> None:
        self._entry(variable, year)["done"] = True
        self._write()

    # --- time sub-chunks --------------------------------------------------------
    def is_chunk_done(self, variable: str, year: int, start: str, end: str) -> bool:
        entry = self._data["var_years"].get(_vy_key(variable, year))
        return bool(entry and _chunk_key(start, end) in entry.get("chunks", []))

    def mark_chunk_done(self, variable: str, year: int, start: str, end: str) -> None:
        chunks = self._entry(variable, year)["chunks"]
        key = _chunk_key(start, end)
        if key not in chunks:
            chunks.append(key)
            chunks.sort()
            self._write()

    # --- spatial sub-chunks -----------------------------------------------------
    # The GEE backend splits a variable-year by *area* as well (src/gee/chunks.py), and
    # those parts share the ``chunks`` list with the time sub-chunks above — the ``sp:``
    # prefix keeps the two namespaces apart. A spatial part never implies the year is
    # done: only the caller that owns the full extent knows when every part has landed,
    # so ``mark_var_year_done`` stays its decision.
    def is_spatial_chunk_done(self, variable: str, year: int, chunk_id: str) -> bool:
        entry = self._data["var_years"].get(_vy_key(variable, year))
        return bool(entry and _spatial_key(chunk_id) in entry.get("chunks", []))

    def mark_spatial_chunk_done(self, variable: str, year: int, chunk_id: str) -> None:
        chunks = self._entry(variable, year)["chunks"]
        key = _spatial_key(chunk_id)
        if key not in chunks:
            chunks.append(key)
            chunks.sort()
            self._write()
