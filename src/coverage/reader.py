"""Readers that turn each hub's processed files into a stream of ``Slice``.

A single :class:`GriddedNCReader` handles every regular-grid nc hub
(Sentinel-5P/3, GEMS, MODIS) — they differ only by the metadata captured in
:class:`~src.coverage.registry.HubSpec`. ERA5 (CSV) and Himawari (mock) get
thin dedicated readers so the engine can treat all hubs uniformly.
"""
from __future__ import annotations

import glob
import os
from datetime import datetime
from pathlib import Path
from typing import Iterator

import numpy as np
import pandas as pd
import xarray as xr

from src.config.settings import BASE_DIR
from .base import Slice
from .registry import HubSpec, get_spec


def _is_real_nc(path: str) -> bool:
    """Skip macOS AppleDouble sidecars (``._foo.nc``) that litter the drive."""
    return not os.path.basename(path).startswith("._")


def _pick(ds: xr.Dataset, names: tuple) -> str | None:
    for n in names:
        if n in ds.variables or n in ds.coords:
            return n
    return None


class GriddedNCReader:
    """Generic reader for regular lon/lat nc cubes, driven by a HubSpec."""

    def __init__(self, spec: HubSpec, base_dir: Path = BASE_DIR):
        self.spec = spec
        self.processed_dir = Path(base_dir) / spec.dir_name / "processed"

    # -- file discovery -----------------------------------------------------
    def iter_files(self, product: str, start: datetime, end: datetime) -> list[str]:
        # Sentinel-5P 的 processed 多一層 level(processed/L2/<PRODUCT>/...);其餘 hub level=None
        root = self.processed_dir / self.spec.level / product if self.spec.level else self.processed_dir / product
        if self.spec.layout == "year_month":
            # Prefilter by YYYY/MM folders so a date range doesn't glob years
            # of irrelevant orbits.
            files = []
            for year in range(start.year, end.year + 1):
                for month in range(1, 13):
                    if datetime(year, month, 1) > end.replace(day=1):
                        continue
                    if (year, month) < (start.year, start.month):
                        continue
                    files += glob.glob(str(root / f"{year}" / f"{month:02d}" / "*.nc"))
        else:  # flat: PRODUCT/*.nc (e.g. MODIS yearly cube)
            files = glob.glob(str(root / "*.nc"))
        return sorted(f for f in files if _is_real_nc(f))

    # -- slice streaming ----------------------------------------------------
    def _resolve_var(self, ds: xr.Dataset, product: str) -> str:
        want = self.spec.variables.get(product)
        if want and want in ds.data_vars:
            return want
        # Fallback: the first/only data variable (robust for S3 / AERAOD).
        return list(ds.data_vars)[0]

    def iter_slices(self, product: str, start: datetime, end: datetime) -> Iterator[Slice]:
        latn_pref, lonn_pref, timen_pref = (
            self.spec.lat_names, self.spec.lon_names, self.spec.time_names)
        for path in self.iter_files(product, start, end):
            try:
                ds = xr.open_dataset(path)
            except Exception:
                continue
            try:
                latn = _pick(ds, latn_pref)
                lonn = _pick(ds, lonn_pref)
                timen = _pick(ds, timen_pref)
                if latn is None or lonn is None:
                    continue
                var = self._resolve_var(ds, product)
                lats = ds[latn].values
                lons = ds[lonn].values
                da = ds[var]

                if timen and timen in da.dims:
                    times = pd.to_datetime(ds[timen].values)
                    for i, t in enumerate(times):
                        if not (start <= t.to_pydatetime() <= end):
                            continue
                        vals = np.asarray(da.isel({timen: i}).values).squeeze()
                        yield Slice(t.to_pydatetime(), vals, lats, lons,
                                    self.spec.key, product)
                else:
                    # No time dim on the variable: take file-level time if any.
                    if timen and timen in ds.coords:
                        t = pd.to_datetime(ds[timen].values).ravel()[0].to_pydatetime()
                    else:
                        t = _time_from_name(path)
                    if t is None or not (start <= t <= end):
                        continue
                    vals = np.asarray(da.values).squeeze()
                    yield Slice(t, vals, lats, lons, self.spec.key, product)
            finally:
                ds.close()


class ERA5Reader:
    """ERA5 processed output is per-station hourly CSV, not gridded nc.

    Coverage here means *temporal availability* (hours present vs. expected),
    not valid-pixel fraction — reanalysis has no cloud gaps. Wired as a stub
    that the engine can call; full implementation is deferred.
    """

    def __init__(self, spec: HubSpec, base_dir: Path = BASE_DIR):
        self.spec = spec
        self.base_dir = Path(base_dir)

    def iter_slices(self, product, start, end):  # pragma: no cover - stub
        import warnings
        warnings.warn(
            "ERA5 coverage = temporal availability of station CSVs (not "
            "valid-pixel fraction); reader not yet implemented — see SCHEMA.md.")
        return iter(())


class MockReader:
    """Himawari hub is still a mock with no processor; yields nothing."""

    def __init__(self, spec: HubSpec, base_dir: Path = BASE_DIR):
        self.spec = spec

    def iter_slices(self, product, start, end):
        import warnings
        warnings.warn(f"Hub '{self.spec.key}' is a mock with no processed data; "
                      "returning no slices.")
        return iter(())


def _time_from_name(path: str) -> datetime | None:
    """Best-effort date from a filename like ``..._20230107T045523_...``."""
    import re
    m = re.search(r"(20\d{6})(?:T(\d{6}))?", os.path.basename(path))
    if not m:
        return None
    d = m.group(1)
    return datetime(int(d[:4]), int(d[4:6]), int(d[6:8]))


def get_reader(hub: str, base_dir: Path = BASE_DIR):
    spec = get_spec(hub)
    if spec.kind == "csv":
        return ERA5Reader(spec, base_dir)
    if spec.kind == "mock":
        return MockReader(spec, base_dir)
    return GriddedNCReader(spec, base_dir)
