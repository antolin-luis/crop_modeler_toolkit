"""Polygon shapefile reader (``src/fields/shp.py``) and the field-row builder.

Shapefiles here are written byte by byte, so the tests pin the format rather than another
library's reading of it. The real Weizen file is read too when present.
"""

from __future__ import annotations

import json
import struct
from pathlib import Path

import pandas as pd
import pytest

from src.config import resolve_data_path
from src.fields import shp
from src.fields.load import build_rows

WEIZEN = Path(__file__).resolve().parent.parent / ".localdata/fields/weizen/Weizen_LG_E2026.shp"


def _polygon_record(rings, shape_type=5) -> bytes:
    pts = [p for r in rings for p in r]
    parts, k = [], 0
    for r in rings:
        parts.append(k)
        k += len(r)
    xs, ys = [p[0] for p in pts], [p[1] for p in pts]
    body = struct.pack("<i4d", shape_type, min(xs), min(ys), max(xs), max(ys))
    body += struct.pack("<ii", len(rings), len(pts))
    body += struct.pack(f"<{len(parts)}i", *parts)
    body += b"".join(struct.pack("<2d", *p) for p in pts)
    if shape_type == 15:  # Z range + values, then M range + values: all skipped
        body += struct.pack(f"<{2 + len(pts)}d", *([0.0] * (2 + len(pts)))) * 2
    return body


def write_shp(path: Path, records, shape_type=5, prj: str | None = None) -> Path:
    bodies = [
        struct.pack("<i", 0) if r is None else _polygon_record(r, shape_type)
        for r in records
    ]
    out = b""
    for n, b in enumerate(bodies, 1):
        out += struct.pack(">ii", n, len(b) // 2) + b
    header = struct.pack(">i5ii", 9994, 0, 0, 0, 0, 0, (100 + len(out)) // 2)
    header += struct.pack("<ii4d4d", 1000, shape_type, 0, 0, 1, 1, 0, 0, 0, 0)
    path.write_bytes(header + out)
    if prj is not None:
        path.with_suffix(".prj").write_text(prj)
    return path


SQUARE = [(0, 0), (0, 1), (1, 1), (1, 0), (0, 0)]
HOLE = [(0.2, 0.2), (0.8, 0.2), (0.8, 0.8), (0.2, 0.8), (0.2, 0.2)]


def test_reads_rings_and_keeps_record_index_across_null(tmp_path):
    p = write_shp(tmp_path / "a.shp", [[SQUARE, HOLE], None, [SQUARE]])
    shapes = shp.read_polygons(p)
    assert [s.index for s in shapes] == [0, 2]
    assert shapes[0].rings == [[(float(x), float(y)) for x, y in SQUARE],
                               [(float(x), float(y)) for x, y in HOLE]]
    assert shapes[0].wkt().startswith("MULTILINESTRING((0.000000000 0.000000000,")
    assert shapes[0].wkt().count("(") == 3


def test_polygon_z_skips_z_and_m(tmp_path):
    p = write_shp(tmp_path / "z.shp", [[SQUARE], [SQUARE]], shape_type=15)
    shapes = shp.read_polygons(p)
    assert len(shapes) == 2 and len(shapes[1].rings[0]) == 5


def test_refuses_projected_prj(tmp_path):
    p = write_shp(tmp_path / "u.shp", [[SQUARE]], prj='PROJCS["WGS 84 / UTM zone 32N"]')
    with pytest.raises(ValueError, match="projected"):
        shp.check_prj(p)


def test_accepts_custom_named_wgs84_prj(tmp_path):
    # The Weizen file's datum name is vendor-specific; the spheroid is what says WGS84.
    p = write_shp(
        tmp_path / "w.shp", [[SQUARE]],
        prj='GEOGCS["MYCUSTOM_TGIS_CSDatum_WGS84",DATUM["x",SPHEROID["WGS_84",6378137]]]',
    )
    shp.check_prj(p)


def test_build_rows_ids_names_attrs():
    attrs = pd.DataFrame(
        {"ID": ["1", "2"], "NAME": ["Kläranlage", "Remise"], "FLAECHE": [9.2, 52.7],
         "V2_FRUCHT": ["WRA", "\x00\x00\x00\x00"]}
    )
    shapes = [shp.Shape(0, [SQUARE]), shp.Shape(1, [SQUARE])]
    rows = build_rows(attrs, shapes, "ID", "NAME")
    assert list(rows["src_id"]) == ["1", "2"]
    assert rows.loc[0, "name"] == "Kläranlage"
    assert json.loads(rows.loc[1, "attrs"])["V2_FRUCHT"] is None


def test_build_rows_refuses_duplicate_ids():
    attrs = pd.DataFrame({"ID": [7.0, 7.0]})
    shapes = [shp.Shape(0, [SQUARE]), shp.Shape(1, [SQUARE])]
    with pytest.raises(ValueError, match="not unique"):
        build_rows(attrs, shapes, "ID", None)


def test_resolve_data_path(monkeypatch):
    monkeypatch.setenv("DATA_DIR", "/data")
    assert str(resolve_data_path("fields/w/x.shp")) == "/data/fields/w/x.shp"
    with pytest.raises(ValueError, match="outside DATA_DIR"):
        resolve_data_path("/home/me/x.shp")


@pytest.mark.skipif(not WEIZEN.exists(), reason="Weizen shapefile not staged")
def test_real_weizen_file():
    shp.check_prj(WEIZEN)
    shapes = shp.read_polygons(WEIZEN)
    assert len(shapes) == 20
    for s in shapes:
        for x, y in (p for r in s.rings for p in r):
            assert 11.6 < x < 12.0 and 51.4 < y < 51.8
