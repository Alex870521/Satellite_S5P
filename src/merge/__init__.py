"""Merge per-time processed nc into one time-series cube, across satellite hubs.

Processors already grid each granule to the hub's fixed lat/lon, so merging is
just: discover processed files for a product/date-range, assign a real ``time``
coordinate, concat along ``time``, write one compressed nc. Reuses the coverage
registry/reader for discovery (Sentinel-5P L2 / GEMS / MODIS); Sentinel-5P L3
(processed/L3/<product>/<aggregation>/) is discovered directly.

Quickstart:
    from src.merge import merge_product
    out = merge_product("gems", "NO2", "2022-01-01", "2023-12-31")

CLI:
    python -m src.merge --hub sentinel5p --product NO2___ \
        --start 2023-01-01 --end 2023-12-31
"""
from .engine import merge_product

__all__ = ["merge_product"]
