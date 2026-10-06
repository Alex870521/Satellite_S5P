"""Raw-L2 QA-threshold coverage sweep (Sentinel-5P).

How does the ``qa_value`` cutoff trade off against spatial coverage? For each
raw TROPOMI L2 granule in a date range this computes — at several qa thresholds
— the fraction of a bounding box's pixels that survive
``qa_value >= threshold`` and are non-NaN.

Graduated from ``wip_coverage/qa_coverage_analysis.py`` (fixes: seeded sampling,
no blocking ``plt.show()``, filled-in report). It deliberately lives in its own
module rather than in :func:`src.coverage.compute_coverage`: that function reads
*processed*, already-QC'd nc and reports per-region coverage over time, whereas
here we read *raw* L2 swaths and the qa cutoff itself IS the variable. Region is
a **bounding box** only — point-in-polygon over a full raw swath is needless.

    python -m src.coverage.qa_sweep --product NO2___ \
        --start 2023-01-01 --end 2023-12-31 --qa 0.5 0.7 0.75 \
        --sample 500 --plot qa_sweep.png --out qa_stats.csv
"""
from __future__ import annotations

import os
import random
from datetime import datetime
from pathlib import Path
from typing import cast

import numpy as np
import pandas as pd
import xarray as xr

from src.config.settings import BASE_DIR, FIGURE_BOUNDARY, REGIONS
from src.config.catalog import PRODUCT_CONFIGS
from src.utils.extract_datetime_from_filename import extract_datetime_from_filename

DEFAULT_QA = (0.5, 0.7, 0.75)
_STAT_COLS = ["qa", "mean", "std", "median", "q25", "q75", "min", "max", "count"]


def _bbox(region):
    """Resolve a region name to a (lon_min, lon_max, lat_min, lat_max) box."""
    if region in (None, "figure"):
        return tuple(FIGURE_BOUNDARY)
    if region in REGIONS:
        return tuple(REGIONS[region])
    raise ValueError(
        f"qa_sweep takes a bounding-box region; got '{region}'. Known boxes: "
        f"'figure', {sorted(REGIONS)} (air-quality polygons aren't supported on "
        "raw swaths).")


def _file_date(path: str) -> datetime | None:
    try:
        t = extract_datetime_from_filename(os.path.basename(path), to_local=False)
    except Exception:
        return None
    if t is not None and t.tzinfo is not None:
        t = t.replace(tzinfo=None)
    return cast("datetime | None", t)


def _raw_files(product: str, start: datetime, end: datetime,
               base_dir: Path) -> list[str]:
    """Raw L2 granules of *product* whose obs time falls in [start, end]."""
    root = Path(base_dir) / "Sentinel-5P" / "raw"
    files = []
    for p in root.glob(f"**/{product}/**/*.nc"):
        if p.name.startswith("._") or not p.is_file():
            continue
        t = _file_date(str(p))
        if t is not None and start <= t <= end:
            files.append(str(p))
    return sorted(files)


def _analyze_file(path: str, product: str, bbox, qa_values) -> dict | None:
    """Per-qa (valid, total) pixel counts inside *bbox* for one granule."""
    var = PRODUCT_CONFIGS[product].dataset_name
    try:
        ds = xr.open_dataset(path, engine="netcdf4", group="PRODUCT")
    except Exception:
        return None
    try:
        data = np.asarray(ds[var].values[0], dtype=float)
        qa = np.asarray(ds["qa_value"].values[0], dtype=float)
        lat = np.asarray(ds["latitude"].values[0], dtype=float)
        lon = np.asarray(ds["longitude"].values[0], dtype=float)
    except Exception:
        return None
    finally:
        ds.close()

    lo_lon, hi_lon, lo_lat, hi_lat = bbox
    in_box = (lon >= lo_lon) & (lon <= hi_lon) & (lat >= lo_lat) & (lat <= hi_lat)
    total = int(in_box.sum())
    if total == 0:
        return None
    return {q: (int((in_box & (qa >= q) & np.isfinite(data)).sum()), total)
            for q in qa_values}


def sweep_qa(product: str = "NO2___", start="2022-01-01", end="2023-12-31", *,
             qa_values=DEFAULT_QA, region="figure", base_dir: Path = BASE_DIR,
             sample_size: int | None = None, seed: int = 0) -> pd.DataFrame:
    """Per-granule coverage (%) at each qa threshold — one row per (file, qa).

    Re-scans every matching raw file each call (idempotent). ``sample_size``
    sub-samples granules with a fixed ``seed`` for reproducibility.
    """
    start = pd.to_datetime(start).to_pydatetime()
    end = pd.to_datetime(end).to_pydatetime()
    if end.hour == end.minute == end.second == 0 and end.microsecond == 0:
        end = end.replace(hour=23, minute=59, second=59)
    bbox = _bbox(region)

    files = _raw_files(product, start, end, base_dir)
    if sample_size and len(files) > sample_size:
        files = sorted(random.Random(seed).sample(files, sample_size))

    rows = []
    for path in files:
        res = _analyze_file(path, product, bbox, qa_values)
        if res is None:
            continue
        t = _file_date(path)
        date = t.strftime("%Y-%m-%d") if t else ""
        for q, (valid, total) in res.items():
            rows.append({"file": os.path.basename(path), "date": date, "qa": q,
                         "valid": valid, "total": total,
                         "coverage": 100.0 * valid / total})
    return pd.DataFrame(rows, columns=["file", "date", "qa", "valid",
                                       "total", "coverage"])


def summarize(per_file: pd.DataFrame) -> pd.DataFrame:
    """Aggregate the per-granule table to per-qa coverage statistics (%)."""
    if per_file.empty:
        return pd.DataFrame(columns=_STAT_COLS)
    g = per_file.groupby("qa")["coverage"]
    s = pd.DataFrame({
        "mean": g.mean(), "std": g.std(), "median": g.median(),
        "q25": g.quantile(0.25), "q75": g.quantile(0.75),
        "min": g.min(), "max": g.max(), "count": g.count(),
    }).reset_index()
    return s.round(2)


def plot_qa_sweep(per_file: pd.DataFrame, output=None, *, dpi=600, title=None):
    """2x2 summary: density histograms, boxplot, mean±std bar, mean-vs-qa line."""
    import matplotlib.pyplot as plt
    import matplotlib

    if per_file.empty:
        raise ValueError("no QA-sweep rows to plot")
    qa_values = sorted(per_file["qa"].unique())
    stats = summarize(per_file).set_index("qa")
    means = np.array([stats.loc[q, "mean"] for q in qa_values])
    stds = np.array([stats.loc[q, "std"] for q in qa_values])
    colors = matplotlib.colormaps["viridis"](np.linspace(0.15, 0.85, len(qa_values)))   # 同 plt.cm.viridis

    fig, axes = plt.subplots(2, 2, figsize=(15, 12))

    ax = axes[0, 0]
    for q, c in zip(qa_values, colors):
        ax.hist(per_file.loc[per_file.qa == q, "coverage"], bins=30,
                density=True, alpha=0.5, color=c, label=f"qa≥{q:.2f}")
    ax.set_xlabel("Coverage (%)", fontsize=13)
    ax.set_ylabel("Density", fontsize=13)
    ax.set_title("Coverage distribution by QA threshold", fontsize=14)
    ax.legend(fontsize=11)

    ax = axes[0, 1]
    ax.boxplot([per_file.loc[per_file.qa == q, "coverage"] for q in qa_values])
    ax.set_xticklabels([f"≥{q:.2f}" for q in qa_values])  # version-agnostic
    ax.set_ylabel("Coverage (%)", fontsize=13)
    ax.set_title("Coverage spread by QA threshold", fontsize=14)

    ax = axes[1, 0]
    ax.bar([f"≥{q:.2f}" for q in qa_values], means, yerr=stds, capsize=5,
           color=colors)
    ax.set_ylabel("Mean coverage (%)", fontsize=13)
    ax.set_title("Mean coverage ±1σ", fontsize=14)

    ax = axes[1, 1]
    ax.plot(qa_values, means, "o-", linewidth=2, markersize=8, color="#8B5CF6")
    ax.fill_between(qa_values, means - stds, means + stds, alpha=0.2,
                    color="#8B5CF6")
    ax.set_xlabel("QA threshold", fontsize=13)
    ax.set_ylabel("Mean coverage (%)", fontsize=13)
    ax.set_title("Coverage vs QA threshold", fontsize=14)

    fig.suptitle(title or "QA-threshold coverage sweep", fontsize=18,
                 fontweight="bold")
    fig.tight_layout()
    if output is not None:
        fig.savefig(output, dpi=dpi, bbox_inches="tight", facecolor="white")
        plt.close(fig)
    return fig


def write_report(per_file: pd.DataFrame, path) -> None:
    """Markdown stats table (the wip version left an unfilled ``XX%`` template)."""
    stats = summarize(per_file)
    lines = ["# QA-threshold coverage sweep", "",
             f"Granules analysed: {per_file['file'].nunique()}", "",
             "| QA≥ | mean % | median % | std | q25 | q75 | min | max | n |",
             "|---|---|---|---|---|---|---|---|---|"]
    for _, r in stats.iterrows():
        lines.append(f"| {r['qa']:.2f} | {r['mean']:.1f} | {r['median']:.1f} | "
                     f"{r['std']:.1f} | {r['q25']:.1f} | {r['q75']:.1f} | "
                     f"{r['min']:.1f} | {r['max']:.1f} | {int(r['count'])} |")
    Path(path).write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv=None):
    import argparse

    p = argparse.ArgumentParser(
        prog="python -m src.coverage.qa_sweep",
        description="Raw-L2 qa_value vs spatial-coverage sweep (Sentinel-5P).")
    p.add_argument("--product", default="NO2___",
                   help="S5P L2 product folder with a qa_value (default NO2___)")
    p.add_argument("--start", default="2022-01-01")
    p.add_argument("--end", default="2023-12-31")
    p.add_argument("--qa", type=float, nargs="+", default=list(DEFAULT_QA),
                   help="qa thresholds to sweep (default 0.5 0.7 0.75)")
    p.add_argument("--region", default="figure",
                   help=f"bbox: 'figure' or {sorted(REGIONS)}")
    p.add_argument("--sample", type=int, default=None,
                   help="randomly sample this many granules (seeded)")
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--out", default=None, help="per-granule CSV path")
    p.add_argument("--plot", default=None, help="2x2 summary PNG path")
    p.add_argument("--report", default=None, help="Markdown stats report path")
    p.add_argument("--base-dir", default=None, help="override BASE_DIR")
    args = p.parse_args(argv)

    base_dir = Path(args.base_dir) if args.base_dir else BASE_DIR
    per_file = sweep_qa(args.product, args.start, args.end,
                        qa_values=tuple(args.qa), region=args.region,
                        base_dir=base_dir, sample_size=args.sample,
                        seed=args.seed)
    if per_file.empty:
        print("No raw granules matched the product/region/time range.")
        return 1

    print(summarize(per_file).to_string(index=False))
    if args.out:
        per_file.to_csv(args.out, index=False)
        print(f"Wrote {len(per_file)} rows -> {args.out}")
    if args.plot:
        plot_qa_sweep(per_file, args.plot,
                      title=f"QA sweep — S5P {args.product} ({args.region})")
        print(f"Wrote plot -> {args.plot}")
    if args.report:
        write_report(per_file, args.report)
        print(f"Wrote report -> {args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
