"""Faithful daily AOD L3 grid from MODIS L2 swath granules (drop-in-the-box binning).

Why this exists
---------------
``MODISProcessor.merge_hdf_files_to_netcdf`` builds a daily cube, but it reprojects
each granule with adaptive **interpolation** (``_reproject_to_taiwan_grid``: linear /
IDW / nearest-neighbour with distance limits). Linear/IDW smooth and invent values
between observations, and the method switches per-day by data density (inconsistent).

This module does **footprint-aware nearest-neighbour resampling** instead. The key
geometry fact: the 0.1° (~11 km) grid is *finer* than a MODIS L2 footprint — a pixel
is ~10 km at nadir but grows to ~40 km cross-track at the swath edge (neighbour spacing
0.09° → 0.43°). So a single coarse pixel COVERS several fine grid cells. Dropping each
pixel into only the one cell holding its centre (plain binning) leaves holes between
centres and makes a contiguous swath look dotty — wrong.

Correct fill: each grid cell takes the value of the nearest L2 pixel **if it lies within
that pixel's own footprint radius** (≈ half the local pixel spacing — small at nadir,
large at the edge). Result:
  * footprint-faithful — every cell the swath actually covers is filled, big edge
    pixels included; cells beyond any footprint stay NaN
  * no smoothing/invention — a cell gets a real pixel value, not an interpolation
  * correct gaps — clouds and between-orbit gaps stay NaN; same-day east+west passes
    complement (all granules pooled before resampling)
  * idempotent — re-scans every raw granule on each run

Use for coverage/analysis. Keeps the interpolated product untouched (writes to its own
``<file_type>_binned`` folder) so the two can be compared before replacing.
"""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from src.config.settings import BASE_DIR
from src.processing.modis_processor import MODISProcessor
from src.utils.extract_datetime_from_filename import extract_datetime_from_filename

DEFAULT_BOUNDS = (119.0, 123.0, 21.0, 26.0)   # lon_min, lon_max, lat_min, lat_max


def _grid_axes(bounds, resolution):
    """Cell-centre 1-D axes."""
    lon_min, lon_max, lat_min, lat_max = bounds
    lon_c = np.arange(lon_min, lon_max + resolution / 2, resolution)
    lat_c = np.arange(lat_min, lat_max + resolution / 2, resolution)
    return lon_c, lat_c


def _footprint_radius(lat2d, lon2d, floor, cap):
    """Per-pixel footprint radius (deg) ≈ half the local cross-track spacing.

    Cross-track is the fast-growing axis (≈0.09° nadir → ≈0.43° edge); half that
    spacing is how far the pixel reaches toward its neighbour. Floored so a nadir
    pixel still reaches its own cell centre, capped to avoid runaway at the very edge.
    """
    coslat = np.cos(np.deg2rad(lat2d))
    # spacing to the next column (cross-track); pad last column with its neighbour
    dlat = np.diff(lat2d, axis=1)
    dlon = np.diff(lon2d, axis=1) * coslat[:, :-1]
    step = np.hypot(dlat, dlon)
    step = np.concatenate([step, step[:, -1:]], axis=1)  # back to full width
    r = 0.5 * step
    return np.clip(r, floor, cap)


def _grid_day_nn(lons, lats, vals, radii, lon_c, lat_c):
    """Footprint-aware nearest-neighbour fill.

    Each grid cell takes the nearest valid pixel's value iff it lies within that
    pixel's footprint radius. Returns (field, covered_mask).
    """
    from scipy.spatial import cKDTree

    ny, nx = len(lat_c), len(lon_c)
    out = np.full((ny, nx), np.nan, dtype="float32")
    finite = np.isfinite(vals) & np.isfinite(lons) & np.isfinite(lats)
    lons, lats, vals, radii = lons[finite], lats[finite], vals[finite], radii[finite]
    if vals.size == 0:
        return out, np.zeros((ny, nx), bool)

    # Work in a locally-equal-area-ish space: scale lon by cos(mean lat) so the
    # degree-distance KDTree is isotropic enough for nearest-neighbour.
    clat = np.cos(np.deg2rad(float(np.mean(lats))))
    tree = cKDTree(np.column_stack([lons * clat, lats]))

    lon_g, lat_g = np.meshgrid(lon_c, lat_c)
    cells = np.column_stack([lon_g.ravel() * clat, lat_g.ravel()])
    dist, idx = tree.query(cells, k=1)
    accept = dist <= radii[idx]          # within the matched pixel's footprint
    flat = out.ravel()
    flat[accept] = vals[idx][accept]
    out = flat.reshape(ny, nx)
    return out, accept.reshape(ny, nx)


def _extractor(file_type: str, raw_dir: Path) -> MODISProcessor:
    """A MODISProcessor wired just enough to reuse its HDF4 reading."""
    p = MODISProcessor()
    p.file_type = file_type
    p.raw_dir = Path(raw_dir)
    p.logger = logging.getLogger("modis_daily_grid")
    return p


def _scan_granules_by_date(raw_dir: Path, file_type: str, start: datetime, end: datetime):
    """{date -> [hdf paths]} for the date range, skipping AppleDouble sidecars."""
    root = Path(raw_dir) / file_type
    by_date: dict = {}
    for f in sorted(root.rglob("*.hdf")):
        if f.name.startswith("._"):
            continue
        d = extract_datetime_from_filename(f.name, to_local=False)
        if d is None or not (start <= d <= end):
            continue
        by_date.setdefault(d.date(), []).append(f)
    return by_date


def build_daily_aod_grid(start, end, file_type: str = "MYD04_L2",
                         base_dir: Path = BASE_DIR, resolution: float = 0.1,
                         bounds=DEFAULT_BOUNDS, out_path: Path | None = None,
                         aod_variable: str = "AOD_550_Dark_Target_Deep_Blue_Combined",
                         verbose: bool = True) -> Path:
    """Scan L2 granules in [start, end], bin to a daily (time, lat, lon) AOD cube.

    Returns the written NetCDF path. Idempotent — rebuilds from a full re-scan.
    """
    start = pd.to_datetime(start).to_pydatetime()
    end = pd.to_datetime(end).to_pydatetime().replace(hour=23, minute=59, second=59)
    base_dir = Path(base_dir)
    raw_dir = base_dir / "MODIS" / "raw"
    lon_c, lat_c = _grid_axes(bounds, resolution)
    # floor radius = half a cell diagonal, so a nadir pixel always fills its own cell
    r_floor = 0.5 * resolution * np.sqrt(2)
    r_cap = 0.35  # ~ widest edge footprint half-spacing

    ex = _extractor(file_type, raw_dir)
    ex.aod_variable = aod_variable
    by_date = _scan_granules_by_date(raw_dir, file_type, start, end)
    if not by_date:
        raise FileNotFoundError(f"No {file_type} granules under {raw_dir} in range.")

    times, fields = [], []
    for d in sorted(by_date):
        plons, plats, pvals, prad = [], [], [], []
        for f in by_date[d]:
            hdf = ex._open_with_pyhdf(f)
            if not hdf:
                continue
            try:
                aod, lat, lon = ex._extract_mod04_data(hdf, hdf.datasets())
            finally:
                ex._close_hdf_file(hdf)
            if aod is None:
                continue
            lat = np.asarray(lat); lon = np.asarray(lon)
            r = _footprint_radius(lat, lon, r_floor, r_cap)
            plons.append(lon.ravel()); plats.append(lat.ravel())
            pvals.append(np.asarray(aod).ravel()); prad.append(r.ravel())
        if not pvals:
            continue
        field, covered = _grid_day_nn(
            np.concatenate(plons), np.concatenate(plats),
            np.concatenate(pvals), np.concatenate(prad), lon_c, lat_c)
        times.append(np.datetime64(d))
        fields.append(field)
        if verbose:
            print(f"{d}  granules={len(by_date[d])}  covered_cells={int(covered.sum())}")

    cube = np.stack(fields, axis=0)
    ds = xr.Dataset(
        {"aod": (("time", "lat", "lon"), cube,
                 {"long_name": "Aerosol Optical Depth", "units": "dimensionless"})},
        coords={"time": np.array(times), "lat": lat_c.astype("float32"),
                "lon": lon_c.astype("float32")},
        attrs={
            "title": f"MODIS {file_type} daily AOD (footprint-aware NN resampled)",
            "method": "footprint-aware nearest-neighbour: each cell = nearest valid "
                      "L2 pixel within that pixel's footprint radius (~half local "
                      "cross-track spacing); all same-day granules pooled; no "
                      "smoothing/interpolation; cells beyond any footprint = NaN",
            "aod_variable": aod_variable,
            "grid_resolution_deg": resolution,
            "bounds_lonmin_lonmax_latmin_latmax": list(bounds),
            "created_from": f"{len(times)} days, re-scanned from raw L2",
        },
    )
    if out_path is None:
        # Own product folder ('<file_type>_binned') so it never sits next to the
        # old interpolated cube — the coverage reader globs a folder per product.
        s, e = sorted(by_date)[0], sorted(by_date)[-1]
        out_path = (base_dir / "MODIS" / "processed" / f"{file_type}_binned" /
                    f"{file_type}_daily_binned_{s:%Y%m%d}_{e:%Y%m%d}.nc")
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    ds.to_netcdf(out_path, encoding={"aod": {"zlib": True, "complevel": 4,
                                             "_FillValue": np.nan}})
    if verbose:
        print(f"\nWrote {len(times)} daily slices -> {out_path}")
    return out_path


def _main(argv=None):
    import argparse
    p = argparse.ArgumentParser(
        prog="python -m src.processing.modis_daily_grid",
        description="Build faithful daily AOD L3 grid (binning, no interpolation).")
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--file-type", default="MYD04_L2")
    p.add_argument("--resolution", type=float, default=0.1)
    p.add_argument("--base-dir", default=str(BASE_DIR))
    p.add_argument("--out", default=None)
    a = p.parse_args(argv)
    build_daily_aod_grid(a.start, a.end, file_type=a.file_type,
                         base_dir=Path(a.base_dir), resolution=a.resolution,
                         out_path=a.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
