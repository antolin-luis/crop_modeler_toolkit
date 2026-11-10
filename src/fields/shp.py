"""Minimal reader for polygon shapefiles — the geometry half ``src/grid/dbf.py`` skips.

Same trade as ``dbf.py``: no GDAL in the Airflow image, and a polygon ``.shp`` is a
simple binary format (ESRI whitepaper, 1998). Only polygon shapes (types 5/15/25) are
read; Z and M values are skipped. Rings are returned as-is — sorting outer rings from
holes is left to PostGIS ``ST_BuildArea``, which does it from geometry rather than from
the ring-orientation convention that real-world files do not always honour.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from pathlib import Path

_FILE_CODE = 9994
_HEADER_SIZE = 100
_NULL = 0
_POLYGON_TYPES = frozenset({5, 15, 25})   # Polygon, PolygonZ, PolygonM

Ring = list[tuple[float, float]]


@dataclass(frozen=True)
class Shape:
    """One record: its 0-based position (matches the DBF row order) and its rings."""

    index: int
    rings: list[Ring]

    def wkt(self) -> str:
        """Rings as a ``MULTILINESTRING`` — the input ``ST_BuildArea`` wants."""
        parts = (
            "(" + ", ".join(f"{x:.9f} {y:.9f}" for x, y in ring) + ")"
            for ring in self.rings
        )
        return "MULTILINESTRING(" + ", ".join(parts) + ")"


def check_prj(shp_path: str | Path) -> None:
    """Refuse anything but geographic WGS84 coordinates.

    A missing ``.prj`` is accepted (the bbox check in ``read_polygons`` still catches
    projected metres). A projected CRS is refused rather than reprojected: that would need
    PROJ on this side, and handing PostGIS UTM metres labelled 4326 would be silent garbage.
    """
    prj = Path(shp_path).with_suffix(".prj")
    if not prj.exists():
        return
    text = prj.read_text(errors="replace").strip().upper()
    if text.startswith("PROJCS"):
        raise ValueError(
            f"{prj.name} is a projected CRS; reproject the shapefile to WGS84 "
            "(EPSG:4326) first"
        )
    if not text.startswith("GEOGCS") or "WGS" not in text or "84" not in text:
        raise ValueError(f"{prj.name} is not geographic WGS84: {text[:80]}...")


def read_polygons(shp_path: str | Path) -> list[Shape]:
    """Every non-null polygon record of ``shp_path``, in file order."""
    data = Path(shp_path).read_bytes()
    if len(data) < _HEADER_SIZE or struct.unpack(">i", data[:4])[0] != _FILE_CODE:
        raise ValueError(f"{shp_path}: not a shapefile")
    shape_type = struct.unpack("<i", data[32:36])[0]
    if shape_type not in _POLYGON_TYPES:
        raise ValueError(f"{shp_path}: shape type {shape_type} is not a polygon type")
    xmin, ymin, xmax, ymax = struct.unpack("<4d", data[36:68])
    if not (-180 <= xmin <= xmax <= 180 and -90 <= ymin <= ymax <= 90):
        raise ValueError(
            f"{shp_path}: bbox {(xmin, ymin, xmax, ymax)} is not lon/lat degrees"
        )

    shapes: list[Shape] = []
    pos, index = _HEADER_SIZE, 0
    while pos + 8 <= len(data):
        _, content_words = struct.unpack(">ii", data[pos : pos + 8])
        start = pos + 8
        pos = start + 2 * content_words
        rec_type = struct.unpack("<i", data[start : start + 4])[0]
        if rec_type != _NULL:
            if rec_type not in _POLYGON_TYPES:
                raise ValueError(f"{shp_path}: record {index} has type {rec_type}")
            n_parts, n_points = struct.unpack("<ii", data[start + 36 : start + 44])
            parts = struct.unpack(f"<{n_parts}i", data[start + 44 : start + 44 + 4 * n_parts])
            p0 = start + 44 + 4 * n_parts
            coords = struct.unpack(f"<{2 * n_points}d", data[p0 : p0 + 16 * n_points])
            points = list(zip(coords[0::2], coords[1::2]))
            bounds = list(parts) + [n_points]
            rings = [points[a:b] for a, b in zip(bounds[:-1], bounds[1:])]
            shapes.append(Shape(index=index, rings=[r for r in rings if len(r) >= 4]))
        index += 1
    return shapes
