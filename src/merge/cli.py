"""CLI for src.merge — merge a hub/product's processed grids into one nc.

    python -m src.merge --hub sentinel5p --product NO2___ \
        --start 2023-01-01 --end 2023-12-31

    # Sentinel-5P L3 (daily NO2 tropospheric):
    python -m src.merge --hub sentinel5p --product no2-tropospheric \
        --level L3 --aggregation day --start 2022-01-01 --end 2023-12-31

    # MODIS yearly cubes across years:
    python -m src.merge --hub modis --product MCD19A2 --start 2022-01-01 --end 2024-12-31
"""
import argparse
from pathlib import Path

from src.coverage.registry import HUB_SPECS
from .engine import merge_product


def main(argv=None):
    p = argparse.ArgumentParser(
        prog="python -m src.merge",
        description="Merge a hub/product's processed grids into one time-series nc.",
    )
    p.add_argument("--hub", required=True, choices=sorted(HUB_SPECS),
                   help="衛星 hub")
    p.add_argument("--product", required=True,
                   help="產品代碼(L2 如 NO2___;L3 如 no2-tropospheric)")
    p.add_argument("--start", required=True, help="YYYY-MM-DD")
    p.add_argument("--end", required=True, help="YYYY-MM-DD")
    p.add_argument("--level", default=None,
                   help="處理階層(Sentinel-5P 限定:L2 預設 / L3)")
    p.add_argument("--aggregation", default=None,
                   help="L3 時間聚合:day/fortnight/month/season/year")
    p.add_argument("--out", default=None, help="輸出 nc 路徑(預設放到該 hub 的 processed/)")
    p.add_argument("--base-dir", default=None, help="覆寫 SATELLITE_BASE_DIR")
    p.add_argument("--no-compress", action="store_true", help="不要 zlib 壓縮")
    args = p.parse_args(argv)

    kw = {}
    if args.base_dir:
        kw["base_dir"] = Path(args.base_dir)
    out = merge_product(
        args.hub, args.product, args.start, args.end,
        level=args.level, aggregation=args.aggregation,
        out=args.out, compress=not args.no_compress, **kw,
    )
    print(out)
    return 0
