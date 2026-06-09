"""Command-line entry for the coverage toolkit.

    python -m src.coverage --hub sentinel5p --product NO2___ \
        --region central --start 2023-01-01 --end 2023-12-31 \
        --granularity daily --weight count --out coverage_no2.csv

Run from the repo root so ``src`` is importable.
"""
from __future__ import annotations

import argparse
import sys

from .engine import compute_coverage
from .metric import WEIGHTS
from .region import AIR_QUALITY_ZONES
from src.config.settings import REGIONS


def main(argv=None):
    p = argparse.ArgumentParser(
        prog="python -m src.coverage",
        description="Regional data-coverage rate over time, per satellite hub.")
    p.add_argument("--hub", required=True,
                   help="sentinel5p | sentinel3 | gems | modis | era5 | himawari")
    p.add_argument("--product", required=True,
                   help="product folder, e.g. NO2___ (S5P), MYD04_L2 (MODIS), NO2 (GEMS)")
    p.add_argument("--region", required=True,
                   help=f"box {sorted(REGIONS)} or zone {sorted(AIR_QUALITY_ZONES)}")
    p.add_argument("--start", required=True, help="YYYY-MM-DD")
    p.add_argument("--end", required=True, help="YYYY-MM-DD")
    p.add_argument("--granularity", default="daily",
                   choices=["per_file", "daily", "monthly", "yearly"])
    p.add_argument("--weight", default="count", choices=list(WEIGHTS))
    p.add_argument("--out", default=None, help="output CSV path (default: stdout summary)")
    p.add_argument("--base-dir", default=None,
                   help="override BASE_DIR (e.g. GEMS lives on a different drive)")
    args = p.parse_args(argv)

    kw = {"granularity": args.granularity, "weight": args.weight}
    if args.base_dir:
        from pathlib import Path
        kw["base_dir"] = Path(args.base_dir)

    df = compute_coverage(args.hub, args.product, args.region,
                          args.start, args.end, **kw)

    if df.empty:
        print("No data found for the given hub/product/region/time range.",
              file=sys.stderr)
        return 1

    if args.out:
        df.to_csv(args.out, index=False)
        print(f"Wrote {len(df)} rows -> {args.out}")
    print(df.to_string(index=False) if len(df) <= 40 else df.describe())
    print(f"\nmean coverage = {df['coverage'].mean():.3f}  "
          f"(n={len(df)} {args.granularity} buckets, "
          f"{df['n_slices'].sum()} slices)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
