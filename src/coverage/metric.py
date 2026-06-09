"""Cell-weighting schemes for the coverage denominator/numerator.

``count``  — every in-region cell counts as 1 (the wip_coverage default).
``area``   — cells weighted by cos(latitude), i.e. true geographic area, so
             coverage isn't biased by the lon-line convergence toward the pole.

The historical ``gaussian`` / ``area_weighted`` CSV variants came from the
*gridding* stage (how L2 pixels are spread onto the target grid), which happens
upstream in the processor — not a cell weight here. Those hooks are left for a
future ``--weight gaussian`` once gridded reading is wired; today both options
operate on already-gridded fields.
"""
from __future__ import annotations

import numpy as np

from .region import cell_lats_2d

WEIGHTS = ("count", "area")


def cell_weights(lats: np.ndarray, lons: np.ndarray, weight: str) -> np.ndarray:
    """2-D array of per-cell weights matching the value field's shape."""
    if weight == "count":
        lat2d = cell_lats_2d(lats, lons)
        return np.ones_like(lat2d, dtype=float)
    if weight == "area":
        lat2d = cell_lats_2d(lats, lons)
        return np.cos(np.deg2rad(lat2d))
    raise ValueError(f"Unknown weight '{weight}'. Known: {WEIGHTS}")
