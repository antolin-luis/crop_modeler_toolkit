"""Canonical **fine** grid specification — CHIRPS 0.05° (see docs/plan_chirps_fine_grid.md).

The second grid in this project. It exists because CHIRPS is a 0.05° product and
aggregating it to the 0.25° ERA5 grid at ingest would throw away the 25x spatial detail
that is the entire reason for adding it.

Like ``src/grid/spec.py``, these are a DOCUMENTED CONSTANT: codes are globally
deterministic, so a fine cell gets the same code no matter which extent was built. Only the
grid *table* is region-scoped (a global 0.05° grid is 17.3M rows — too big to ship as a seed
the way ``era5_land_base_grid`` is).

DO NOT CHANGE these values for the life of the database.

Two ways this grid differs from the 0.25° one, both load-bearing:

1. **Cell EDGES fall on multiples of the resolution**, not centres. ERA5 centres sit on
   multiples of 0.25 (so ``encoding`` uses ``round``); CHIRPS *edges* sit on multiples of
   0.05 and centres at ``(i + 0.5) * 0.05`` (so ``fine_encoding`` uses ``floor``). Getting
   this wrong shifts every value by half a cell — output that looks entirely plausible and
   is entirely wrong.
2. **Codes are 5 chars wide**, because 2400*7200 = 17,280,000 does not fit the 4-char space
   (36**4 = 1,679,616). This doubles as a type tag: a 4-char code is *always* the 0.25° ERA5
   grid, a 5-char code is *always* this one. The two can never be silently confused.

The two grids do **not** nest. ERA5 cell edges land on ``0.125 + k*0.25``, which is not a
multiple of 0.05, so no arithmetic maps one to the other — moving between them is an
area-weighted spatial join (``chirps_era5_map``), never a division.
"""

# The alphabet is imported rather than redeclared: it is the same base-36 alphabet, and two
# copies that could drift apart would corrupt codes on one grid only, silently.
from src.grid.spec import ALPHABET

RESOLUTION = 0.05  # degrees
NLON = 7200        # 360 / 0.05
NLAT = 2400        # 120 / 0.05 — CHIRPS v3 spans 60S..60N (v2's 50S..50N is a row subset)

# Latitude index origin: row 0 is the northernmost row, spanning 60.00 down to 59.95.
LAT_ORIGIN = 60.0

# Longitude index origin, in the 0..360 convention — index 0 spans lon 0.00..0.05. Matches
# the ``lon % 360`` idiom the 0.25° encoder already uses, so both grids index longitude the
# same way. NOT to be confused with GEOTIFF_LON_CORNER below: one is a code-space origin,
# the other is a raster-layout corner. They are different concerns that both want the word
# "origin".
LON_ORIGIN = 0.0

# The exported GeoTIFF's top-left corner, i.e. the ``crsTransform`` used at export time.
# Mirrored in src/gee/export.py:FINE_CRS_TRANSFORM, which is what actually reaches EE.
GEOTIFF_LON_CORNER = -180.0
GEOTIFF_LAT_CORNER = 60.0

CODE_WIDTH = 5
CODE_SPACE = len(ALPHABET) ** CODE_WIDTH  # 60,466,176 > 17,280,000 cells

# Parent block factor: a parent covers BLOCK_B x BLOCK_B fine cells. At 20 that is a 1.0°
# box holding exactly 400 cells — the same footprint as a b=4 parent on the 0.25° grid, so
# both grids partition the world into the same 1° boxes and reason about it the same way.
#
# IMMUTABLE for the life of the table: it is baked into every fparent_id and every partition
# name (PLANNING.md §6.3 makes the same point for the 0.25° grid's b).
#
# Two clean properties follow from 2400 and 7200 both being divisible by 20, neither of
# which the 0.25° grid enjoys (NLAT=721 is not divisible by 4):
#   - every parent holds exactly 400 cells; there are no short edge blocks
#   - parent boundaries land on integer degrees, so the antimeridian falls exactly ON a
#     boundary and no block ever straddles it
BLOCK_B = 20
