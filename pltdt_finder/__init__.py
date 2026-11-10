"""Planting-date finder: estimates sowing dates from per-pixel Sentinel-2 NDVI series.

Reads what the ``s2_ndvi_fields`` DAG writes (``field_parcel`` / ``s2_pixel`` /
``s2_ndvi_obs``); see ``io.py``.
"""
