"""Core data structures for the coverage toolkit.

A *coverage rate* answers: of the grid cells that fall inside a region of
interest, what fraction carry valid (non-NaN, QC-passed) data at a given time?

Every supported satellite hub stores its processed product differently
(per-orbit single-time files vs. one multi-time cube, ``latitude`` vs ``lat``
coord names, ...). To keep the coverage engine hub-agnostic, each reader
normalises its files into a stream of :class:`Slice` objects — one 2-D field
with its grid and timestamp — and the engine only ever sees ``Slice``.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import numpy as np


@dataclass
class Slice:
    """A single 2-D field on a lon/lat grid at one instant.

    ``lats``/``lons`` are 1-D for the regular grids every hub currently writes
    (S5P/S3/GEMS use a fixed GridFrame, MODIS is pre-gridded to 0.1°). The
    engine also accepts 2-D coordinate arrays (raw swath) should a future
    reader emit them.
    """

    time: datetime
    values: np.ndarray            # 2-D (nlat, nlon)
    lats: np.ndarray              # 1-D (nlat,) or 2-D
    lons: np.ndarray              # 1-D (nlon,) or 2-D
    hub: str = ""
    product: str = ""

    def grid_signature(self) -> tuple:
        """Identity of the grid, so a region mask can be cached and reused.

        Within one hub+product the grid is fixed, so this is cheap and stable.
        """
        return (
            self.lats.shape, self.lons.shape,
            float(self.lats.flat[0]), float(self.lats.flat[-1]),
            float(self.lons.flat[0]), float(self.lons.flat[-1]),
        )


@dataclass
class CoverageRow:
    """One row of the tidy output table."""

    hub: str
    product: str
    region: str
    time: str          # ISO date / month / year string for the bucket
    granularity: str   # per_file | daily | monthly | yearly
    weight: str        # count | area
    valid: float       # weighted count of covered cells
    total: float       # weighted count of region cells
    coverage: float    # valid / total
    n_slices: int = 1  # how many raw slices folded into this bucket
