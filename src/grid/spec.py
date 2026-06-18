"""Canonical grid specification (PLANNING.md §6.1).

These constants define the deterministic global 0.25° grid. They are a DOCUMENTED
CONSTANT: the global-consistency guarantee depends on every component using exactly
this spec. A shifted origin silently produces divergent codes across regions.

DO NOT CHANGE these values for the life of the database.
"""

RESOLUTION = 0.25  # degrees
LON_ORIGIN = 0.0   # longitudes 0.00 .. 359.75
LAT_ORIGIN = 90.0  # latitudes 90.00 .. -90.00 (descending)
NLON = 1440        # 360 / 0.25
NLAT = 721         # 180 / 0.25 + 1
ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"  # base-36

# Capacity check (PLANNING.md §6.1): the whole globe fits in a 4-char base-36 code.
#   721 * 1440 = 1_038_240 cells < 36**4 = 1_679_616
CODE_SPACE = len(ALPHABET) ** 4

# Land-clip coastal-inclusiveness knob (PLANNING.md §6.4). A 0.25° cell is marked
# land if at least this fraction of the ERA5-Land (0.1°) centroids falling inside its
# footprint are land. Higher -> stricter (drops thin coastal cells); lower -> more
# inclusive of coastline. Tune for coastal agriculture, not a hard physical constant.
LAND_FRACTION_X = 0.5
