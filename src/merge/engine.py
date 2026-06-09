"""Merge a hub's per-time processed nc files into one time-series cube.

The processors already grid each granule to the hub's fixed lat/lon, so merging
is just: discover processed files for a product/date-range, give each a real
``time`` coordinate, concat along ``time``, and write one compressed nc.

Reuses the coverage registry/reader for discovery (same per-hub knowledge) for
the gridded hubs (Sentinel-5P L2, GEMS, MODIS). Sentinel-5P **L3** lives under a
different layout (``processed/L3/<product>/<aggregation>/``) and is discovered
directly here.

Quickstart:
    from src.merge import merge_product
    out = merge_product("sentinel5p", "NO2___", "2023-01-01", "2023-12-31")
    # Sentinel-5P L3:
    out = merge_product("sentinel5p", "no2-tropospheric", "2022-01-01",
                        "2023-12-31", level="L3", aggregation="day")
"""
from __future__ import annotations

import glob
import re
from datetime import datetime
from pathlib import Path

import numpy as np
import xarray as xr

from src.config.settings import BASE_DIR
from src.coverage.registry import get_spec
from src.coverage.reader import get_reader

# L3 檔名末段 -<資料日>-<上架日>.nc;一般檔名最後退而求其次抓任一 8 位日期
_L3_DATE = re.compile(r"-(\d{8})-\d{8}\.nc$")
_ANY_DATE = re.compile(r"(\d{8})")


def _norm(d) -> datetime:
    return d if isinstance(d, datetime) else datetime.strptime(d, "%Y-%m-%d")


def _date_from_name(name: str) -> datetime | None:
    m = _L3_DATE.search(name) or _ANY_DATE.search(name)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y%m%d")
    except ValueError:
        return None


def _discover(hub: str, product: str, start: datetime, end: datetime,
              level: str | None, aggregation: str | None, base_dir: Path) -> list[str]:
    """List processed nc files for the product/date-range."""
    # Sentinel-5P L3:processed/L3/<product>/<aggregation>/*.nc(結構與 coverage 不同)
    if hub in ("sentinel5p", "sentinel3") and (level or "").upper() == "L3":
        spec = get_spec(hub)
        root = Path(base_dir) / spec.dir_name / "processed" / "L3" / product
        if aggregation:
            root = root / aggregation
        files = [f for f in glob.glob(str(root / "**" / "*.nc"), recursive=True)
                 if "/._" not in f]
        # 用檔名日期過濾到範圍內
        out = []
        for f in files:
            d = _date_from_name(Path(f).name)
            if d is None or (start <= d <= end):
                out.append(f)
        return sorted(out)
    # 其餘(S5P L2 / GEMS / MODIS):複用 coverage reader 的探索
    return list(get_reader(hub, base_dir).iter_files(product, start, end))


def _load_with_time(path: str) -> xr.Dataset:
    """Open one file ensuring a datetime64 ``time`` coord/dim (derive from name if missing)."""
    ds = xr.open_dataset(path)
    t = ds.coords.get("time")
    if t is not None and np.issubdtype(t.dtype, np.datetime64):
        return ds  # 已是真實時間(S5P L2 / GEMS / MODIS cube)
    # 否則從檔名取日期(S5P L3:每檔一天,time 只是 index)
    d = _date_from_name(Path(path).name) or datetime(1970, 1, 1)
    if "time" not in ds.dims:
        ds = ds.expand_dims("time")
    n = ds.sizes["time"]
    return ds.assign_coords(time=("time", np.array([np.datetime64(d, "ns")] * n)))


def _default_out(hub: str, product: str, level: str | None,
                 start: datetime, end: datetime, base_dir: Path) -> Path:
    spec = get_spec(hub)
    proc = Path(base_dir) / spec.dir_name / "processed"
    if (level or "").upper() == "L3":
        proc = proc / "L3" / product
    tag = f"{start:%Y%m%d}_{end:%Y%m%d}"
    safe = product.replace("/", "-")
    return proc / f"{spec.dir_name}_{safe}_merged_{tag}.nc"


def merge_product(hub: str, product: str, start, end, *,
                  level: str | None = None, aggregation: str | None = None,
                  out: str | Path | None = None, base_dir: Path = BASE_DIR,
                  compress: bool = True, return_dataset: bool = False):
    """Merge one hub/product's processed grids over [start, end] into a single nc.

    level/aggregation: only for Sentinel-5P L3 (e.g. level='L3', aggregation='day').
    out: output path; default = <processed>/<Dir>_<product>_merged_<range>.nc.
    Returns the output Path (or the merged Dataset if return_dataset=True).
    """
    s, e = _norm(start), _norm(end)
    files = _discover(hub, product, s, e, level, aggregation, base_dir)
    if not files:
        raise FileNotFoundError(
            f"merge: 找不到 {hub}/{product} 在 {s:%Y-%m-%d}~{e:%Y-%m-%d} 的 processed 檔"
            f"{' (level=L3 '+str(aggregation)+')' if level else ''}")

    print(f"[merge] {hub}/{product}: 合併 {len(files)} 個檔…", flush=True)
    dss = [_load_with_time(f) for f in files]
    # 各檔同一固定網格 → join='override' 直接沿 time 串接(免對齊、較快)
    merged = xr.concat(dss, dim="time", join="override", combine_attrs="drop_conflicts")
    merged = merged.sortby("time")
    # 限制到查詢窗(含整個 end 當天)
    merged = merged.sel(time=slice(np.datetime64(s, "ns"),
                                   np.datetime64(e.replace(hour=23, minute=59, second=59), "ns")))

    merged.attrs.update({
        "merged_by": "src.merge",
        "hub": hub, "product": product,
        "level": level or "L2",
        "time_range": f"{s:%Y-%m-%d}..{e:%Y-%m-%d}",
        "n_source_files": len(files),
    })

    if return_dataset:
        return merged

    out_path = Path(out) if out else _default_out(hub, product, level, s, e, base_dir)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    enc = ({v: {"zlib": True, "complevel": 4} for v in merged.data_vars}
           if compress else None)
    merged.to_netcdf(out_path, encoding=enc)
    print(f"[merge] 已輸出: {out_path}  dims={dict(merged.sizes)}", flush=True)
    return out_path
