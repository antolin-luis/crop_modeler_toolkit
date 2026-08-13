"""dBase III reader tests.

The reader replaces a geopandas/fiona dependency, so what it must not do is silently
mis-parse: every conversion, the deletion flag, and the chunk boundary are pinned here
against DBF bytes built by hand in memory. Nothing touches the filesystem except the
round-trip through a real ``.dbf`` and a real ``.zip`` at the end.
"""

from __future__ import annotations

import io
import struct
import zipfile

import numpy as np
import pandas as pd
import pytest

from src.grid import dbf


def make_dbf(fields, records, *, deleted=()):
    """Assemble a minimal dBase III file.

    ``fields`` is ``[(name, type, length, decimals), ...]``; ``records`` is a list of lists
    of already-formatted (unpadded) strings. ``deleted`` holds the indices to tombstone.
    """
    header_len = 32 + 32 * len(fields) + 1
    record_len = 1 + sum(f[2] for f in fields)

    out = bytearray()
    out += struct.pack("<B3BIHH", 0x03, 26, 1, 1, len(records), header_len, record_len)
    out += b"\x00" * 20  # reserved
    for name, typ, length, decimals in fields:
        out += name.encode("latin-1").ljust(11, b"\x00")[:11]
        out += typ.encode("latin-1")
        out += b"\x00" * 4  # field data address, unused
        out += bytes([length, decimals])
        out += b"\x00" * 14
    out += b"\x0d"  # field-descriptor terminator

    for i, record in enumerate(records):
        out += b"*" if i in deleted else b" "
        for value, (_n, _t, length, _d) in zip(record, fields, strict=True):
            out += value.encode("latin-1").rjust(length)[:length]
    out += b"\x1a"  # EOF marker; the reader stops on n_records and never reads it
    return bytes(out)


FIELDS = [("CELL5M", "N", 9, 0), ("SoilProfil", "C", 12, 0), ("X", "F", 12, 6)]
RECORDS = [
    ["2455938", "AD02455938", "1.542000"],
    ["2455939", "AD02455939", "-61.792000"],
    ["2460257", "AD02460257", "-85.625000"],
]


def read_all(raw, **kwargs):
    fh = io.BytesIO(raw)
    header = dbf.read_header(fh)
    chunks = list(dbf.iter_chunks(fh, header, **kwargs))
    return header, pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()


def test_header_parses_fields_and_offsets():
    header = dbf.read_header(io.BytesIO(make_dbf(FIELDS, RECORDS)))

    assert header.n_records == 3
    assert header.record_len == 1 + 9 + 12 + 12
    assert [f.name for f in header.fields] == ["CELL5M", "SoilProfil", "X"]
    assert [f.typ for f in header.fields] == ["N", "C", "F"]
    # Offsets start at 1: byte 0 of every record is the deletion flag.
    assert [f.start for f in header.fields] == [1, 10, 22]
    assert header.field("X").decimals == 6


def test_values_round_trip_with_types():
    _header, df = read_all(make_dbf(FIELDS, RECORDS))

    assert len(df) == 3
    # N with 0 decimals -> nullable integer, not float; cell5m is a key, not a measurement.
    assert df["CELL5M"].dtype == "Int64"
    assert df["CELL5M"].tolist() == [2455938, 2455939, 2460257]
    # C is right-stripped of the format's blank padding.
    assert df["SoilProfil"].tolist() == ["AD02455938", "AD02455939", "AD02460257"]
    assert df["X"].dtype == np.float64
    assert df["X"].tolist() == pytest.approx([1.542, -61.792, -85.625])


def test_blank_numeric_becomes_na_not_zero():
    """The format's NULL is a blank-padded field. Reading it as 0.0 would put a point
    on Null Island instead of failing, so it must come back as NA."""
    _header, df = read_all(make_dbf(FIELDS, [["2455938", "AD02455938", ""]]))

    assert pd.isna(df.loc[0, "X"])
    assert df.loc[0, "CELL5M"] == 2455938


def test_deleted_records_are_dropped():
    _header, df = read_all(make_dbf(FIELDS, RECORDS, deleted={1}))

    assert df["CELL5M"].tolist() == [2455938, 2460257]


def test_chunking_splits_without_losing_or_duplicating_rows():
    records = [[str(1000 + i), f"X{i:04d}", f"{i}.5"] for i in range(250)]
    fh = io.BytesIO(make_dbf(FIELDS, records))
    header = dbf.read_header(fh)

    chunks = list(dbf.iter_chunks(fh, header, chunk_rows=100))

    assert [len(c) for c in chunks] == [100, 100, 50]
    assert pd.concat(chunks, ignore_index=True)["CELL5M"].tolist() == [
        1000 + i for i in range(250)
    ]


def test_chunk_size_dividing_the_record_count_exactly():
    records = [[str(1000 + i), f"X{i:04d}", f"{i}.5"] for i in range(200)]
    fh = io.BytesIO(make_dbf(FIELDS, records))
    header = dbf.read_header(fh)

    chunks = list(dbf.iter_chunks(fh, header, chunk_rows=100))

    assert [len(c) for c in chunks] == [100, 100]  # no trailing empty chunk


def test_columns_restricts_what_is_materialized():
    _header, df = read_all(make_dbf(FIELDS, RECORDS), columns=["CELL5M", "X"])

    assert list(df.columns) == ["CELL5M", "X"]


def test_unknown_column_raises():
    fh = io.BytesIO(make_dbf(FIELDS, RECORDS))
    header = dbf.read_header(fh)

    with pytest.raises(KeyError, match="NOPE"):
        list(dbf.iter_chunks(fh, header, columns=["NOPE"]))


def test_unsupported_type_raises_rather_than_guessing():
    fields = [("CELL5M", "N", 9, 0), ("BLOB", "M", 10, 0)]
    fh = io.BytesIO(make_dbf(fields, [["2455938", "0000000001"]]))
    header = dbf.read_header(fh)

    with pytest.raises(ValueError, match="unsupported DBF type"):
        list(dbf.iter_chunks(fh, header))

    # ...but selecting around it works, so one exotic column cannot block a whole file.
    fh = io.BytesIO(make_dbf(fields, [["2455938", "0000000001"]]))
    header = dbf.read_header(fh)
    (df,) = list(dbf.iter_chunks(fh, header, columns=["CELL5M"]))
    assert df["CELL5M"].tolist() == [2455938]


def test_logical_and_date_columns():
    fields = [("FLAG", "L", 1, 0), ("WHEN", "D", 8, 0)]
    _header, df = read_all(make_dbf(fields, [["T", "20261015"], ["F", "20260101"]]))

    assert df["FLAG"].tolist() == [True, False]
    assert df["WHEN"].tolist() == [pd.Timestamp("2026-10-15"), pd.Timestamp("2026-01-01")]


def test_truncated_header_raises():
    with pytest.raises(ValueError, match="truncated DBF"):
        dbf.read_header(io.BytesIO(b"\x03\x1a\x01"))


def test_open_table_reads_a_dbf_from_a_zip(tmp_path):
    """The production path: the member is read in place, never extracted."""
    raw = make_dbf(FIELDS, RECORDS)
    archive = tmp_path / "layer.shp.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("layer.dbf", raw)
        zf.writestr("layer.prj", "GEOGCS[...]")
        zf.writestr("layer.shx", b"\x00" * 8)

    with dbf.open_table(archive) as (header, fh):
        assert header.n_records == 3
        (df,) = list(dbf.iter_chunks(fh, header))
    assert df["SoilProfil"].tolist() == ["AD02455938", "AD02455939", "AD02460257"]


def test_zip_with_several_dbf_members_needs_an_explicit_pick(tmp_path):
    archive = tmp_path / "two.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("a.dbf", make_dbf(FIELDS, RECORDS))
        zf.writestr("b.dbf", make_dbf(FIELDS, RECORDS[:1]))

    with pytest.raises(ValueError, match="pass member="):
        with dbf.open_table(archive):
            pass

    with dbf.open_table(archive, "b.dbf") as (header, _fh):
        assert header.n_records == 1


def test_shp_path_is_redirected_to_its_sidecar_dbf(tmp_path):
    (tmp_path / "layer.dbf").write_bytes(make_dbf(FIELDS, RECORDS))
    (tmp_path / "layer.shp").write_bytes(b"not read")

    with dbf.open_table(tmp_path / "layer.shp") as (header, _fh):
        assert header.n_records == 3
