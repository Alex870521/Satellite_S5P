"""Coverage computation engine.

Pipeline: reader -> per-slice region/valid masks -> daily union -> bucket
aggregation -> tidy DataFrame.

A *day* is the atomic unit: several orbits/granules can observe the same day,
so a cell counts as covered if **any** slice that day carries valid data there
(union) — matching the wip_coverage "combined" merge. Coarser granularities
(monthly/yearly) average the daily coverages. ``per_file`` skips the union and
reports every raw slice (diagnostic).
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from src.config.settings import BASE_DIR
from .base import CoverageRow
from .metric import cell_weights
from .reader import get_reader
from .region import region_mask


def _norm_dt(x) -> datetime:
    if isinstance(x, datetime):
        return x
    return pd.to_datetime(x).to_pydatetime()


def compute_coverage(hub: str, product: str, region: str,
                     start, end, granularity: str = "daily",
                     weight: str = "count", base_dir: Path = BASE_DIR) -> pd.DataFrame:
    """Return a tidy coverage table for one hub/product/region/time-range.

    Re-scans every matching file on each call (idempotent) — consistent with the
    "raw files keep getting re-uploaded, tools must be re-runnable" principle.
    """
    start, end = _norm_dt(start), _norm_dt(end)
    # A bare end date means "through end of that day" — otherwise intraday
    # slices (GEMS geostationary, e.g. 00:45) would fall past a midnight cutoff.
    if end.hour == end.minute == end.second == 0 and end.microsecond == 0:
        end = end.replace(hour=23, minute=59, second=59)
    reader = get_reader(hub, base_dir)

    # Per-day accumulators keyed by date.
    valid_acc: dict = {}        # date -> bool union of covered cells
    meta: dict = defaultdict(int)   # date -> n_slices
    # mask/weights cached per grid signature. Within a clean product the grid is
    # fixed; a second signature means the product folder mixes grids (e.g. an old
    # interpolated cube next to a binned one) and union is meaningless -> error.
    grids: dict = {}
    grid = {"weights": None, "mask": None, "total": None, "sig": None}

    per_file_rows: list[CoverageRow] = []

    for sl in reader.iter_slices(product, start, end):
        sig = sl.grid_signature()
        if sig not in grids:
            mask = region_mask(sl.lats, sl.lons, region, sig)
            w = cell_weights(sl.lats, sl.lons, weight)
            grids[sig] = {"mask": mask, "weights": w, "total": float(w[mask].sum())}
        g = grids[sig]
        valid = np.isfinite(sl.values) & g["mask"]

        if granularity == "per_file":
            v = float(g["weights"][valid].sum())
            per_file_rows.append(_row(hub, product, region, sl.time, "per_file",
                                      weight, v, g["total"], 1))
            continue

        if grid["sig"] is None:
            grid.update(g, sig=sig)
        elif sig != grid["sig"]:
            raise ValueError(
                f"Product '{product}' mixes grids ({grid['mask'].shape} vs "
                f"{g['mask'].shape}) — its folder likely holds files on different "
                "grids (e.g. old interpolated + new binned). Point --product at a "
                "folder with one cube.")

        d = sl.time.date()
        if d not in valid_acc:
            valid_acc[d] = np.zeros_like(g["mask"], dtype=bool)
        valid_acc[d] |= valid
        meta[d] += 1

    if granularity == "per_file":
        return _to_df(per_file_rows)

    total = grid["total"] if grid["mask"] is not None else float("nan")
    weights = grid["weights"]

    # Daily rows
    daily_rows = []
    for d in sorted(valid_acc):
        v = float(weights[valid_acc[d]].sum())
        daily_rows.append(_row(hub, product, region,
                               datetime(d.year, d.month, d.day), "daily",
                               weight, v, total, meta[d]))
    df = _to_df(daily_rows)
    if df.empty or granularity == "daily":
        return df

    return _aggregate(df, hub, product, region, weight, granularity)


def _aggregate(daily: pd.DataFrame, hub, product, region, weight, granularity):
    """Average daily coverage up to monthly/yearly buckets."""
    freq = {"monthly": "MS", "yearly": "YS"}[granularity]
    s = daily.copy()
    s["time"] = pd.to_datetime(s["time"])
    g = s.set_index("time").groupby(pd.Grouper(freq=freq))
    rows = []
    for ts, chunk in g:
        if chunk.empty:
            continue
        rows.append(_row(hub, product, region, ts.to_pydatetime(), granularity,
                         weight, float(chunk["valid"].mean()),
                         float(chunk["total"].iloc[0]),
                         int(chunk["n_slices"].sum()),
                         coverage=float(chunk["coverage"].mean())))
    return _to_df(rows)


def _row(hub, product, region, t: datetime, gran, weight, valid, total,
         n_slices, coverage=None) -> CoverageRow:
    cov = coverage if coverage is not None else (valid / total if total else float("nan"))
    label = (t.strftime("%Y-%m-%d") if gran in ("daily", "per_file")
             else t.strftime("%Y-%m") if gran == "monthly" else t.strftime("%Y"))
    if gran == "per_file":
        label = t.strftime("%Y-%m-%d %H:%M")
    return CoverageRow(hub, product, region, label, gran, weight,
                       round(valid, 3), round(total, 3), round(cov, 5), n_slices)


def _to_df(rows: list[CoverageRow]) -> pd.DataFrame:
    cols = ["hub", "product", "region", "time", "granularity", "weight",
            "valid", "total", "coverage", "n_slices"]
    if not rows:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame([r.__dict__ for r in rows])[cols]
