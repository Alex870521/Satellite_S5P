"""Plotting helpers for coverage tables.

Consumes the tidy DataFrame returned by :func:`compute_coverage` (one row per
time bucket, ``coverage`` in 0..1) and renders the **coverage-rate
distribution** — the graduated form of
``wip_coverage/plot_coverage_analysis.py``'s ``plot_coverage_distribution``.

The wip prototype hard-wired three panels (central / zhu-miao / combined) from
wide columns. Here the panels are driven generically by a column of the tidy
table (``region`` by default), so the same function handles one region or a
concatenation of several — e.g.::

    import pandas as pd
    from src.coverage import compute_coverage, plot_coverage_distribution

    df = pd.concat(
        compute_coverage("sentinel5p", "NO2___", z, "2023-01-01", "2023-12-31")
        for z in ("central", "zhumiao")
    )
    plot_coverage_distribution(df, "coverage_dist.png")
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

# Per-granularity noun for the "Frequency (...)" y-axis label.
_GRAN_UNIT = {"daily": "days", "monthly": "months", "yearly": "years",
              "per_file": "slices"}

# Coverage-rate bins for representative-day selection (low, high, label).
# Mirrors wip_coverage/plot_coverage_analysis.py; ``high`` of the top bin is
# nudged past 1.0 so a perfect 1.00 day still lands in "Excellent".
DEFAULT_COVERAGE_BINS = [
    (0.0, 0.3, "Low (0-30%)"),
    (0.3, 0.5, "Medium-Low (30-50%)"),
    (0.5, 0.7, "Medium (50-70%)"),
    (0.7, 0.85, "Good (70-85%)"),
    (0.85, 1.0001, "Excellent (85-100%)"),
]


def plot_coverage_distribution(df, output=None, *, threshold=0.7,
                               facet="region", bins=20, title=None, dpi=600):
    """Histogram(s) of per-bucket coverage from a ``compute_coverage`` table.

    One subplot per distinct value of ``facet`` (default ``"region"``); falls
    back to a single panel when the column is absent or constant. ``coverage``
    is treated as a fraction in ``[0, 1]``.

    Parameters
    ----------
    df : pandas.DataFrame
        Tidy coverage table (needs at least a ``coverage`` column).
    output : str | pathlib.Path, optional
        If given, the figure is saved here (and closed); otherwise it is
        returned for further tweaking.
    threshold : float | None
        Fraction (0..1) drawn as a dashed reference line with an above-threshold
        tally. ``None`` omits it.
    facet : str
        Column to split panels by.
    bins : int
        Histogram bins spanning ``[0, 1]``.
    title : str, optional
        Figure-level suptitle.
    dpi : int
        Save resolution.

    Returns
    -------
    matplotlib.figure.Figure
    """
    import matplotlib.pyplot as plt

    if df.empty:
        raise ValueError("empty coverage table — nothing to plot")

    groups = (list(df.groupby(facet, sort=False))
              if facet in df.columns and df[facet].nunique() > 1
              else [(None, df)])
    n = len(groups)
    fig, axes = plt.subplots(1, n, figsize=(6 * n, 5), squeeze=False)
    for ax, (name, g) in zip(axes[0], groups):
        _draw_hist(ax, g, name, threshold, bins)
    if title:
        fig.suptitle(title, fontsize=18, fontweight="bold")
    fig.tight_layout()

    if output is not None:
        fig.savefig(output, dpi=dpi, bbox_inches="tight", facecolor="white")
        plt.close(fig)
    return fig


def _draw_hist(ax, g, name, threshold, bins):
    cov = g["coverage"].to_numpy(dtype=float)
    cov = cov[np.isfinite(cov)]
    if cov.size == 0:
        raise ValueError("no finite coverage values to plot")

    gran = g["granularity"].iloc[0] if "granularity" in g.columns else "daily"
    unit = _GRAN_UNIT.get(gran, gran)

    ax.hist(cov, bins=np.linspace(0, 1, bins + 1),
            color="#8B5CF6", alpha=0.75, edgecolor="white")
    ax.set_xlim(0, 1)
    ax.set_xlabel("Coverage Rate", fontsize=14)
    ax.set_ylabel(f"Frequency ({unit})", fontsize=14)
    ax.tick_params(labelsize=12)

    # Panel title: the facet value, or hub/product/region when unfaceted.
    if name is not None:
        label = str(name)
    else:
        label = " / ".join(str(g[c].iloc[0]) for c in ("hub", "product", "region")
                           if c in g.columns and g[c].nunique() == 1)
    ax.set_title(label, fontsize=15, fontweight="bold")

    extra = ""
    if threshold is not None:
        ax.axvline(threshold, color="red", linestyle="--", linewidth=2,
                   label=f"{threshold:.0%} threshold")
        ax.legend(fontsize=11)
        n_above = int((cov >= threshold).sum())
        extra = f"\n≥{threshold:.0%}: {n_above}/{cov.size} " \
                f"({n_above / cov.size * 100:.1f}%)"

    stats = (f"Mean: {cov.mean() * 100:.1f}%\n"
             f"Median: {np.median(cov) * 100:.1f}%\n"
             f"Std: {cov.std() * 100:.1f}%{extra}")
    ax.text(0.04, 0.96, stats, transform=ax.transAxes, fontsize=11,
            va="top", bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.6))


# --------------------------------------------------------------------------- #
# Representative days
#
# The wip prototype's representative-day maps drew flux-divergence *emissions*
# and raw L2 pixels — both products of the wip_emission engine, out of scope for
# this lightweight processed-nc toolkit. What graduates here is (1) the
# coverage-driven day *selection* (it consumes the tidy table directly) and
# (2) a generic map of the *processed product field* on the selected days. No
# emission engine, no ERA5 wind, no raw L2.
# --------------------------------------------------------------------------- #

def select_representative_days(df, bins=None):
    """Pick one day per coverage bin — the day closest to the bin's midpoint.

    Expects a daily ``compute_coverage`` table (``coverage`` 0..1, ``time`` a
    date). Returns a tidy frame ``[date, coverage, label]`` (one row per bin
    that had any day). Bins default to :data:`DEFAULT_COVERAGE_BINS`.
    """
    if df.empty:
        raise ValueError("empty coverage table — no days to select")
    bins = bins or DEFAULT_COVERAGE_BINS
    out = []
    for low, high, label in bins:
        sub = df[(df["coverage"] >= low) & (df["coverage"] < high)]
        if sub.empty:
            continue
        mid = (low + high) / 2
        row = sub.loc[(sub["coverage"] - mid).abs().idxmin()]
        out.append({"date": str(row["time"]), "coverage": float(row["coverage"]),
                    "label": label})
    return pd.DataFrame(out, columns=["date", "coverage", "label"])


def _day_field(reader, product, date):
    """Union same-day orbits into one 2-D field (nanmean over overlaps)."""
    d = pd.to_datetime(date).to_pydatetime()
    start = d.replace(hour=0, minute=0, second=0, microsecond=0)
    end = d.replace(hour=23, minute=59, second=59, microsecond=0)
    stack, lats, lons = [], None, None
    for sl in reader.iter_slices(product, start, end):
        stack.append(np.asarray(sl.values, dtype=float))
        lats, lons = sl.lats, sl.lons
    if not stack:
        return None
    arr = np.stack(stack, axis=0)
    with np.errstate(invalid="ignore"):
        # all-NaN columns -> NaN (not a warning-raising mean of empty slice)
        field = np.where(np.all(np.isnan(arr), axis=0), np.nan,
                         np.nanmean(arr, axis=0))
    return lats, lons, field


def _region_extent(region, pad=0.15):
    """(lon_min, lon_max, lat_min, lat_max) for a named box or zone polygon."""
    from src.config.settings import REGIONS
    from .region import AIR_QUALITY_ZONES, _zone_polygon
    if region in REGIONS:
        lo_lon, hi_lon, lo_lat, hi_lat = REGIONS[region]
    elif region in AIR_QUALITY_ZONES:
        minx, miny, maxx, maxy = _zone_polygon(region).bounds
        lo_lon, hi_lon, lo_lat, hi_lat = minx, maxx, miny, maxy
    else:
        raise ValueError(f"Unknown region '{region}'")
    return (lo_lon - pad, hi_lon + pad, lo_lat - pad, hi_lat + pad)


def _draw_region(ax, region, transform):
    """Outline the region (zone polygon or bbox rectangle) on a cartopy axes."""
    from src.config.settings import REGIONS
    from .region import AIR_QUALITY_ZONES, _zone_polygon
    if region in AIR_QUALITY_ZONES:
        ax.add_geometries([_zone_polygon(region)], transform, facecolor="none",
                          edgecolor="#111", linewidth=1.8)
    elif region in REGIONS:
        from matplotlib.patches import Rectangle
        lo_lon, hi_lon, lo_lat, hi_lat = REGIONS[region]
        ax.add_patch(Rectangle((lo_lon, lo_lat), hi_lon - lo_lon, hi_lat - lo_lat,
                               fill=False, edgecolor="#111", linewidth=1.8,
                               transform=transform))


def plot_representative_days(df, *, base_dir=None, bins=None, output=None,
                             cmap="YlOrRd", vmax=None, dpi=600):
    """Map the processed product field on each coverage-representative day.

    ``df`` is a *daily* ``compute_coverage`` table for a single hub/product/
    region; the panels read that hub's processed nc for each selected day and
    show the field (same-day orbits unioned). One panel per populated bin.
    """
    import matplotlib.pyplot as plt
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature

    from src.config.settings import BASE_DIR
    from .reader import get_reader

    for col in ("hub", "product", "region"):
        if col in df.columns and df[col].nunique() > 1:
            raise ValueError(f"plot_representative_days needs a single {col}; "
                             f"got {sorted(df[col].unique())}")
    hub = df["hub"].iloc[0]
    product = df["product"].iloc[0]
    region = df["region"].iloc[0]
    base_dir = Path(base_dir) if base_dir is not None else BASE_DIR

    days = select_representative_days(df, bins)
    if days.empty:
        raise ValueError("no representative days selected")

    reader = get_reader(hub, base_dir)
    fields = [_day_field(reader, product, d) for d in days["date"]]

    if vmax is None:
        finite = np.concatenate([f[2][np.isfinite(f[2])].ravel()
                                 for f in fields if f is not None] or [np.array([1.0])])
        vmax = float(np.nanpercentile(finite, 98)) if finite.size else 1.0

    proj = ccrs.PlateCarree()
    extent = _region_extent(region)
    n = len(days)
    fig, axes = plt.subplots(1, n, figsize=(4.2 * n, 5),
                             subplot_kw={"projection": proj}, squeeze=False)
    im = None
    for ax, (_, drow), fld in zip(axes[0], days.iterrows(), fields):
        ax.set_extent(extent, crs=proj)
        ax.add_feature(cfeature.COASTLINE, linewidth=0.5)
        if fld is not None:
            lats, lons, vals = fld
            im = ax.pcolormesh(lons, lats, vals, cmap=cmap, vmin=0, vmax=vmax,
                               shading="auto", transform=proj)
        _draw_region(ax, region, proj)
        ax.set_title(f"{drow['date']}\n{drow['label']}\n"
                     f"Coverage: {drow['coverage'] * 100:.1f}%", fontsize=12)

    if im is not None:
        fig.colorbar(im, ax=list(axes[0]), shrink=0.7,
                     label=f"{hub} {product}")
    fig.suptitle(f"{region} — representative days by coverage", fontsize=16,
                 fontweight="bold")

    if output is not None:
        fig.savefig(output, dpi=dpi, bbox_inches="tight", facecolor="white")
        plt.close(fig)
    return fig


# --------------------------------------------------------------------------
# Raw L2 footprint polygons
#
# Everything above reads *processed* grids. This reads **raw L2** and draws each
# sounding as its actual footprint quadrilateral, which is the only view that
# shows what the instrument really sampled — pixel skew, the widening toward
# the swath edge, and the genuine gaps between orbits. Useful for eyeballing
# what the supersampling regridder in ``src.processing.l3`` is binning.
#
# Graduated from ``wip_coverage/plot_raw_L2_coverage.py``. Changes made on the
# way in: the per-pixel Python loop is vectorised (that prototype looped over
# every sounding twice, once to test the bbox and once to build the polygon),
# the hard-coded ``raw/NO2___`` path is replaced by the current
# ``raw/L2/<product>/`` layout, and the qa threshold and unit scaling are
# arguments rather than literals baked into the body.
# --------------------------------------------------------------------------

def load_raw_l2(path, product="NO2___", *, qa_threshold=0.75, scale=None):
    """Read one raw L2 granule as (values, lat_bounds, lon_bounds).

    ``lat_bounds``/``lon_bounds`` are ``(scanline, ground_pixel, 4)`` footprint
    corners straight from the file — not derived from centres — so the polygons
    are the instrument's own geometry.

    Soundings below ``qa_threshold`` become NaN. ``scale`` multiplies the values
    (e.g. ``6.022e23 / 1e4 / 1e15`` turns mol/m2 into 1e15 molec/cm2); ``None``
    leaves the native unit alone.
    """
    import xarray as xr
    from src.config.catalog import PRODUCT_CONFIGS

    var = PRODUCT_CONFIGS[product].dataset_name
    with xr.open_dataset(path, group="PRODUCT") as ds:
        if var not in ds:
            return None
        vals = np.asarray(ds[var].values).squeeze()
        qa = np.asarray(ds["qa_value"].values).squeeze() if "qa_value" in ds else None
    try:
        with xr.open_dataset(path, group="PRODUCT/SUPPORT_DATA/GEOLOCATIONS") as geo:
            lat_b = np.asarray(geo["latitude_bounds"].values).squeeze()
            lon_b = np.asarray(geo["longitude_bounds"].values).squeeze()
    except (OSError, KeyError):
        return None

    if qa is not None:
        vals = np.where(qa >= qa_threshold, vals, np.nan)
    if scale is not None:
        vals = vals * scale
    if vals.ndim != 2 or lat_b.shape[:2] != vals.shape:
        return None
    return vals, lat_b, lon_b


def plot_raw_l2_pixels(ax, values, lat_bounds, lon_bounds, extent, *,
                       cmap="YlOrRd", vmin=0, vmax=None):
    """Draw each valid sounding as its footprint quadrilateral on *ax*.

    Returns the ``PolyCollection`` (or None when nothing falls inside
    *extent* = ``(lon_min, lon_max, lat_min, lat_max)``).
    """
    import cartopy.crs as ccrs
    from matplotlib.collections import PolyCollection

    lon_min, lon_max, lat_min, lat_max = extent
    lat_c = lat_bounds.mean(axis=2)
    lon_c = lon_bounds.mean(axis=2)
    keep = (np.isfinite(values)
            & (lat_c >= lat_min) & (lat_c <= lat_max)
            & (lon_c >= lon_min) & (lon_c <= lon_max))
    if not keep.any():
        return None

    # (n_kept, 4, 2) corner array — no per-pixel Python loop
    verts = np.stack([lon_bounds[keep], lat_bounds[keep]], axis=-1)
    coll = PolyCollection(list(verts), array=values[keep], cmap=cmap,
                          edgecolors="none", transform=ccrs.PlateCarree())
    coll.set_clim(vmin, np.nanpercentile(values[keep], 98) if vmax is None else vmax)
    ax.add_collection(coll)
    return coll


def plot_raw_l2_coverage(date, *, product="NO2___", region="central",
                         base_dir=None, qa_threshold=0.75, scale=None,
                         output=None, cmap="YlOrRd", vmax=None, dpi=600):
    """One panel per raw L2 granule covering *date*, drawn as footprints.

    Complements :func:`plot_representative_days` (processed grids): this shows
    the un-gridded soundings, so orbit gaps and edge-pixel growth stay visible
    instead of being filled in by regridding.
    """
    import matplotlib.pyplot as plt
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature
    from src.config.settings import BASE_DIR
    from .qa_sweep import _file_date

    base = Path(base_dir) if base_dir else Path(BASE_DIR)
    day = pd.to_datetime(date).date()
    root = base / "Sentinel-5P" / "raw"
    files = sorted(str(p) for p in root.glob(f"**/{product}/**/*.nc")
                   if not p.name.startswith("._")
                   and (_file_date(str(p)) or datetime(1900, 1, 1)).date() == day)
    if not files:
        raise FileNotFoundError(f"{product}: no raw granule found for {day}")

    proj = ccrs.PlateCarree()
    extent = _region_extent(region)
    loaded = [(f, load_raw_l2(f, product, qa_threshold=qa_threshold, scale=scale))
              for f in files]
    loaded = [(f, d) for f, d in loaded if d is not None]
    if not loaded:
        raise ValueError(f"{product} {day}: granules found but none readable")

    fig, axes = plt.subplots(1, len(loaded), figsize=(4.6 * len(loaded), 5),
                             subplot_kw={"projection": proj}, squeeze=False)
    coll = None
    for ax, (fpath, (vals, lat_b, lon_b)) in zip(axes[0], loaded):
        ax.set_extent(extent, crs=proj)
        ax.add_feature(cfeature.COASTLINE, linewidth=0.5)
        c = plot_raw_l2_pixels(ax, vals, lat_b, lon_b, extent, cmap=cmap, vmax=vmax)
        coll = c if c is not None else coll
        _draw_region(ax, region, proj)
        n_in = 0 if c is None else len(c.get_array())
        ax.set_title(f"{Path(fpath).name.split('_')[8][:15]}\n{n_in:,} soundings",
                     fontsize=11)

    if coll is not None:
        fig.colorbar(coll, ax=list(axes[0]), shrink=0.7, label=f"{product} (qa>={qa_threshold})")
    fig.suptitle(f"Raw L2 footprints — {product} {day} ({region})",
                 fontsize=15, fontweight="bold")
    if output is not None:
        fig.savefig(output, dpi=dpi, bbox_inches="tight", facecolor="white")
        plt.close(fig)
    return fig
