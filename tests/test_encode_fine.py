"""Fine-grid raster encoder — src/grid/encode_fine.py.

The headline test here is ``test_raster_on_the_era5_grid_is_rejected``. An export submitted
with the 0.25° crsTransform comes back as a raster that reads, encodes, and loads without
complaint — every value silently resampled off a grid that is not CHIRPS's. That defect is
the most expensive one available on this path, so it gets a hard guard rather than a
tolerance.
"""

import numpy as np
import pandas as pd
import pytest

from src.grid.encode_fine import ALIGNMENT_TOLERANCE_DEG, check_alignment, encode_fine_grid
from src.grid.fine_encoding import fine_code, fine_parent_code
from src.grid.fine_spec import LAT_ORIGIN, RESOLUTION


def fine_axes(lat_idx0: int, lon_idx0: int, nlat: int, nlon: int):
    """Coordinate axes on exact fine-cell centres, as a correct export returns them."""
    lat = LAT_ORIGIN - (np.arange(lat_idx0, lat_idx0 + nlat) + 0.5) * RESOLUTION
    lon = (np.arange(lon_idx0, lon_idx0 + nlon) + 0.5) * RESOLUTION
    lon = np.where(lon > 180.0, lon - 360.0, lon)
    return lat, lon


# Three cells inside Tocantins.
LAT, LON = fine_axes(1404, 6232, 3, 3)
TIMES = pd.to_datetime(["2020-01-01", "2020-01-02"])
VALUES = np.arange(2 * 3 * 3, dtype=float).reshape(2, 3, 3)


def test_encodes_to_the_fine_schema():
    frame = encode_fine_grid(VALUES, LAT, LON, TIMES)
    assert list(frame.columns) == ["fine_id", "fparent_id", "date", "value"]
    assert len(frame) == 2 * 3 * 3


def test_encoded_frames_feed_the_metrics_accumulator():
    """The encoder's output and the cost accumulator must agree on the cell column.

    They did not: ``note_encode_chunk`` hardcoded ERA5's ``child_id``, so every CHIRPS
    bronze task raised ``KeyError`` after its export had completed and spent EECU. Both
    sides were unit-tested; nothing tested the seam, which is where the break was. Uses the
    real encoder and the real RunMetrics — a mock on either side would restore the gap.
    """
    from src.gee.metrics import RunMetrics

    m = RunMetrics(
        kind="bronze_var_year",
        dataset="UCSB-CHC/CHIRPS/V3/DAILY_RNL",
        name_prefix="bronze-gee/chirps_v3_rnl_2020",
        extent=[-13.50, -50.75, -5.15, -45.70],
        cell_column="fine_id",
    )
    m.note_encode_chunk(encode_fine_grid(VALUES, LAT, LON, TIMES))
    rec = m.to_record()
    assert rec["cells"] == 9  # 3x3
    assert rec["days"] == 2
    assert rec["bronze_rows"] == 18


def test_codes_match_the_scalar_encoder():
    frame = encode_fine_grid(VALUES, LAT, LON, TIMES)
    first = frame.iloc[0]
    assert first["fine_id"] == fine_code(LAT[0], LON[0])
    assert first["fparent_id"] == fine_parent_code(LAT[0], LON[0])
    assert all(len(c) == 5 for c in frame["fine_id"])


def test_values_land_on_the_right_cell_and_day():
    """A transposed or mis-tiled reshape would still produce a full, wrong-looking frame."""
    frame = encode_fine_grid(VALUES, LAT, LON, TIMES).set_index(["fine_id", "date"])
    for t, day in enumerate(TIMES.date):
        for i in range(3):
            for j in range(3):
                code = fine_code(LAT[i], LON[j])
                assert frame.loc[(code, day), "value"] == VALUES[t, i, j]


def test_one_row_per_cell_per_day():
    frame = encode_fine_grid(VALUES, LAT, LON, TIMES)
    assert not frame.duplicated(["fine_id", "date"]).any()


def test_hourly_data_is_rejected():
    """Same guard as the 0.25° encoder: two values for one cell-day means not daily."""
    times = pd.to_datetime(["2020-01-01T00:00", "2020-01-01T12:00"])
    with pytest.raises(ValueError, match="not daily"):
        encode_fine_grid(VALUES, LAT, LON, times, source="probe")


# --- alignment guard ---------------------------------------------------------------

def test_correctly_aligned_raster_passes():
    check_alignment(LAT, LON)  # must not raise


def test_raster_on_the_era5_grid_is_rejected():
    """The 0.25° crsTransform puts pixel centres on multiples of 0.25, not on fine centres.

    This is what a CHIRPS export submitted without FINE_CRS_TRANSFORM looks like coming
    back. It must fail loudly rather than encode resampled values.
    """
    era5_lat = np.array([-10.25, -10.50, -10.75])
    era5_lon = np.array([-48.25, -48.00, -47.75])
    with pytest.raises(ValueError, match="crsTransform"):
        check_alignment(era5_lat, era5_lon, source="bronze-gee/chirps_v3_rnl_2020")


def test_half_cell_shift_is_rejected():
    """Edges-vs-centres confusion: pixel centres on multiples of 0.05 instead of offset."""
    shifted_lat = LAT + RESOLUTION / 2
    with pytest.raises(ValueError, match="not on the 0.05"):
        check_alignment(shifted_lat, LON)


def test_float_noise_within_tolerance_is_accepted():
    """GeoTIFF affine arithmetic is not bit-exact; the guard must not fire on that."""
    noisy_lat = LAT + ALIGNMENT_TOLERANCE_DEG / 10
    check_alignment(noisy_lat, LON)


def test_encode_refuses_a_misaligned_raster():
    """The guard has to be wired into encode_fine_grid, not merely available."""
    with pytest.raises(ValueError, match="crsTransform"):
        encode_fine_grid(
            VALUES,
            np.array([-10.25, -10.50, -10.75]),
            np.array([-48.25, -48.00, -47.75]),
            TIMES,
        )


def test_error_message_names_the_source():
    """Across ~182 mapped tasks the message has to say which export was wrong."""
    with pytest.raises(ValueError, match="chirps_v3_rnl_2020"):
        check_alignment(
            LAT + RESOLUTION / 2, LON, source="bronze-gee/chirps_v3_rnl_2020"
        )


def test_antimeridian_axes_encode():
    """Longitude wraps to -180..180 on storage; the guard compares in the raster's own."""
    lat, lon = fine_axes(1200, 7198, 1, 2)
    values = np.zeros((1, 1, 2))
    frame = encode_fine_grid(values, lat, lon, pd.to_datetime(["2020-01-01"]))
    assert len(frame) == 2
    assert all(len(c) == 5 for c in frame["fine_id"])
