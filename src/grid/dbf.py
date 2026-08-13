"""Streaming reader for dBase III ``.dbf`` tables — the attribute half of a shapefile.

Why hand-rolled rather than geopandas/fiona/pyogrio: none of them are in the Airflow
image (``airflow/Dockerfile``), and adding them drags GDAL wheels onto a Raspberry Pi 5
for a rebuild that costs more than this file does to maintain. The project has already
made this trade once — ``seed_grid.assign_timezone`` uses ``timezonefinder`` instead of a
geopandas point-in-polygon join for the same reason. dBase III is a fixed-width format
with a 32-byte header and a 32-byte descriptor per field; that is the whole spec here.

The reader is **streaming and columnar**. The SoilGrids-for-DSSAT source is a 1.1 GB DBF
inside a 36 MB zip: it is read straight out of the zip without ever being extracted, in
chunks, and each chunk is viewed as a numpy structured array so the per-record work stays
in C rather than in a two-million-iteration Python loop. Peak RSS is a function of
``chunk_rows``, not of file size.

Point shapefiles that carry their coordinates as attributes (X/Y columns) need no ``.shp``
parsing at all, which is why only the ``.dbf`` half is implemented.
"""

from __future__ import annotations

import struct
import zipfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import IO

import numpy as np
import pandas as pd

_HEADER_FMT = "<B3BIHH"       # version, yy, mm, dd, n_records, header_len, record_len
_HEADER_SIZE = 32
_FIELD_SIZE = 32
_TERMINATOR = 0x0D            # ends the field-descriptor array
_DELETED = b"*"               # records tombstoned in place; live records carry b" "
_ENCODING = "latin-1"         # dBase III has no encoding declaration; latin-1 never raises

# Type codes this reader converts. Anything else raises rather than being guessed at —
# a silently mis-parsed attribute column is worse than a hard failure.
_SUPPORTED = frozenset("CNFDL")


@dataclass(frozen=True)
class DbfField:
    """One column, plus its byte offset inside a record (deletion flag included)."""

    name: str
    typ: str
    length: int
    decimals: int
    start: int


@dataclass(frozen=True)
class DbfHeader:
    n_records: int
    header_len: int
    record_len: int
    fields: tuple[DbfField, ...]

    def field(self, name: str) -> DbfField:
        for f in self.fields:
            if f.name == name:
                return f
        raise KeyError(f"no field {name!r}; have {[f.name for f in self.fields]}")


def _read_exact(fh: IO[bytes], n: int) -> bytes:
    """Read exactly ``n`` bytes, or as many as remain at EOF.

    ``ZipExtFile.read`` already loops internally, but a short read on a truncated archive
    would otherwise desynchronize every record after it, so the loop is explicit.
    """
    out = bytearray()
    while len(out) < n:
        block = fh.read(n - len(out))
        if not block:
            break
        out += block
    return bytes(out)


def read_header(fh: IO[bytes]) -> DbfHeader:
    """Parse the table header and leave ``fh`` positioned at the first record."""
    raw = _read_exact(fh, _HEADER_SIZE)
    if len(raw) < _HEADER_SIZE:
        raise ValueError("truncated DBF: header is shorter than 32 bytes")
    _version, _yy, _mm, _dd, n_records, header_len, record_len = struct.unpack(
        _HEADER_FMT, raw[:12]
    )
    if header_len < _HEADER_SIZE or record_len < 1:
        raise ValueError(f"implausible DBF header: {header_len=} {record_len=}")

    descriptors = _read_exact(fh, header_len - _HEADER_SIZE)
    fields: list[DbfField] = []
    offset = 1  # byte 0 of every record is the deletion flag
    for i in range(0, len(descriptors) - 1, _FIELD_SIZE):
        chunk = descriptors[i : i + _FIELD_SIZE]
        if not chunk or chunk[0] == _TERMINATOR:
            break
        name = chunk[:11].split(b"\x00")[0].decode(_ENCODING).strip()
        fields.append(
            DbfField(
                name=name,
                typ=chr(chunk[11]),
                length=chunk[16],
                decimals=chunk[17],
                start=offset,
            )
        )
        offset += chunk[16]

    if not fields:
        raise ValueError("DBF header declares no fields")
    if offset > record_len:
        raise ValueError(
            f"field widths sum to {offset} but the record is {record_len} bytes"
        )
    return DbfHeader(n_records, header_len, record_len, tuple(fields))


def _record_dtype(header: DbfHeader) -> np.dtype:
    """A structured dtype whose itemsize is exactly one record.

    Every field is kept as raw bytes (``S<n>``); decoding happens per column, per chunk,
    and only for the columns the caller asked for. Trailing slack (some writers pad the
    record) becomes an unnamed filler so the itemsize still matches ``record_len``.
    """
    spec: list[tuple[str, str]] = [("_deleted", "S1")]
    spec += [(f.name, f"S{f.length}") for f in header.fields]
    used = 1 + sum(f.length for f in header.fields)
    if used < header.record_len:
        spec.append(("_pad", f"S{header.record_len - used}"))
    return np.dtype(spec)


def _decode(values: np.ndarray) -> pd.Series:
    return pd.Series(values).str.decode(_ENCODING).str.strip()


def _convert(field: DbfField, values: np.ndarray) -> pd.Series:
    """Raw fixed-width bytes for one column -> a typed pandas Series."""
    if field.typ not in _SUPPORTED:
        raise ValueError(
            f"field {field.name!r} has unsupported DBF type {field.typ!r}; "
            f"supported: {''.join(sorted(_SUPPORTED))}"
        )

    text = _decode(values)
    if field.typ == "C":
        return text
    if field.typ in "NF":
        # Blank-padded numerics are the format's NULL. errors="coerce" turns them into NaN
        # rather than exploding on a single empty cell in a two-million-row file.
        num = pd.to_numeric(text, errors="coerce")
        if field.typ == "N" and field.decimals == 0:
            return num.astype("Int64")
        return num.astype("float64")
    if field.typ == "D":
        return pd.to_datetime(text, format="%Y%m%d", errors="coerce")
    # "L": logical. dBase writes T/t/Y/y and F/f/N/n; "?" means unset.
    return text.str.upper().map({"T": True, "Y": True, "F": False, "N": False}).astype(
        "boolean"
    )


def iter_chunks(
    fh: IO[bytes],
    header: DbfHeader,
    *,
    chunk_rows: int = 100_000,
    columns: list[str] | None = None,
) -> Iterator[pd.DataFrame]:
    """Yield the records as DataFrames of at most ``chunk_rows`` rows.

    ``fh`` must be positioned at the first record (i.e. straight out of ``read_header``).
    ``columns`` restricts what is materialized — useful both to save work and to skip a
    column whose type code this reader refuses.

    Deleted records are dropped, so a chunk can come back shorter than ``chunk_rows`` (or
    empty) without meaning end-of-file.
    """
    dtype = _record_dtype(header)
    wanted = [f for f in header.fields if columns is None or f.name in columns]
    if columns is not None:
        missing = set(columns) - {f.name for f in header.fields}
        if missing:
            raise KeyError(f"no such field(s): {sorted(missing)}")

    remaining = header.n_records
    while remaining > 0:
        take = min(chunk_rows, remaining)
        block = _read_exact(fh, take * header.record_len)
        n_read = len(block) // header.record_len
        if n_read == 0:
            break

        records = np.frombuffer(block, dtype=dtype, count=n_read)
        live = records["_deleted"] != _DELETED
        records = records[live] if not live.all() else records

        yield pd.DataFrame(
            {f.name: _convert(f, records[f.name]) for f in wanted},
            copy=False,
        )
        remaining -= n_read


@contextmanager
def open_member(
    path: str | Path, member: str | None = None
) -> Iterator[IO[bytes]]:
    """Open a ``.dbf`` byte stream from a zip, a ``.dbf``, or a shapefile's ``.shp``.

    Zips are read in place — a 1.1 GB member is never unpacked to disk. When ``member`` is
    omitted the zip must contain exactly one ``.dbf``.
    """
    path = Path(path)
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as zf:
            name = member
            if name is None:
                names = [n for n in zf.namelist() if n.lower().endswith(".dbf")]
                if len(names) != 1:
                    raise ValueError(
                        f"{path.name} holds {len(names)} .dbf members {names}; "
                        "pass member= to pick one"
                    )
                name = names[0]
            with zf.open(name) as fh:
                yield fh
        return

    if path.suffix.lower() == ".shp":
        path = path.with_suffix(".dbf")
    with path.open("rb") as fh:
        yield fh


@contextmanager
def open_table(
    path: str | Path, member: str | None = None
) -> Iterator[tuple[DbfHeader, IO[bytes]]]:
    """``open_member`` + ``read_header``: the usual entry point.

    Yields the parsed header (record count is known up front, so callers can report
    progress) and the stream positioned at the first record, ready for ``iter_chunks``.
    """
    with open_member(path, member) as fh:
        yield read_header(fh), fh
