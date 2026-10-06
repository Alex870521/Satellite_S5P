"""L3 ingest:已格網化的 L3 檔 → 對齊 canonical GridSpec → GriddedField。

這是「資料已是 L3 → 跳過 regrid」分支。雖然跳過超取樣 regrid,仍會:
  讀 → 對齊網格(與目標 GridSpec 相同則 passthrough;不同則 xarray 線性內插到
  目標 cell 中心,等同裁到網格範圍)→ 包成與 L2 路徑相同的 GriddedField
  (value + count),下游 writer / L3Accumulator 完全共用。

對齊到同一 GridSpec 是跨 source 疊圖/時間聚合能逐格對齊的前提(與 S5P 自己
regrid 出來的 L3 落在同一 lattice)。
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import xarray as xr

from src.config.catalog import ProductConfig
from src.utils.nc_names import pick_name as _pick
from src.processing.l3.granule import GridSpec, GriddedField

_LAT_NAMES = ("latitude", "lat")
_LON_NAMES = ("longitude", "lon")


def _resolve_var(ds: xr.Dataset, product: ProductConfig) -> str:
    want = product.dataset_name
    if want in ds.data_vars:
        return want
    cands = [v for v in ds.data_vars if v != "count"]
    if not cands:
        raise ValueError("L3 檔找不到資料變數")
    return str(cands[0])


def read_l3(nc_file: str | Path, product: ProductConfig):
    """讀 L3 grid → (value DataArray[lat, lon] 升序, time)。"""
    ds = xr.open_dataset(nc_file)
    try:
        latn, lonn = _pick(ds, _LAT_NAMES), _pick(ds, _LON_NAMES)
        if latn is None or lonn is None:
            raise ValueError(f"{nc_file} 不是規則網格(無 1D lat/lon)")
        da = ds[_resolve_var(ds, product)]

        timev = None
        if "time" in ds.coords and ds["time"].size:
            timev = np.datetime64(np.ravel(ds["time"].values)[0], "ns")

        # 壓掉 lat/lon 以外的維(time/layer…取第一片)
        extra = [d for d in da.dims if d not in (latn, lonn)]
        if extra:
            da = da.isel({d: 0 for d in extra})
        da = da.rename({latn: "lat", lonn: "lon"}).sortby("lat").sortby("lon")
        return da.astype(float), timev
    finally:
        ds.close()


def ingest_l3(nc_file: str | Path, grid: GridSpec, product: ProductConfig, *,
              source: str = "", file_name: str | None = None) -> GriddedField:
    """讀 L3 檔並對齊到 ``grid``。網格相同則 passthrough,否則線性內插對齊。"""
    da, timev = read_l3(nc_file, product)
    tlat, tlon = grid.lat, grid.lon

    same = (da["lat"].size == tlat.size and da["lon"].size == tlon.size
            and np.allclose(da["lat"].values, tlat, atol=1e-6)
            and np.allclose(da["lon"].values, tlon, atol=1e-6))
    if same:
        value = da.values
        method = "l3_passthrough"
    else:
        # 線性內插到目標中心;落在來源範圍外 → NaN(等同裁到網格範圍)
        value = da.interp(lat=tlat, lon=tlon).values
        method = "l3_align"

    count = np.isfinite(value).astype(float)
    if timev is None:
        timev = np.datetime64("NaT", "ns")
    return GriddedField(value=value, count=count, grid=grid, product=product,
                        time=timev, source=source or "L3",
                        file_name=file_name or Path(nc_file).name, method=method)
