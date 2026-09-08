"""L2/L3 level 自動偵測。

判斷一個原始檔是 **L2 swath**(需要跑 L3 regrid)還是**已格網化的 L3**
(可跳過 regrid)。判定只看檔案結構,source-agnostic,不靠副檔名/路徑:

  * lat/lon 為 **1D 單調座標** → 規則網格 → ``"L3"``
  * lat/lon 為 **2D**、或有 ``scanline``/``ground_pixel`` 維、或資料藏在
    ``PRODUCT`` group(S5P L2)、或座標藏在 ``Geolocation Fields`` group
    (GEMS L2:``Latitude``/``Longitude`` 2D)→ swath → ``"L2"``

唯一的副檔名特例是 **HDF4**(``.hdf``):xarray 開不了,而本套件裡的 HDF4 一律是
MODIS swath/tile 原始檔,沒有已格網化的 HDF4 來源 → 直接判 ``"L2"``。
"""
from __future__ import annotations

from pathlib import Path

import xarray as xr

_LAT_NAMES = ("latitude", "lat")


def _root_latname(ds: xr.Dataset) -> str | None:
    for n in _LAT_NAMES:
        if n in ds.variables or n in ds.coords:
            return n
    return None


def detect_level(nc_file: str | Path) -> str:
    """回傳 ``"L2"`` 或 ``"L3"``;無法判斷則 raise ValueError。"""
    nc_file = Path(nc_file)

    # HDF4:xarray 開不了,且本套件的 HDF4 一律是 MODIS 原始 swath/tile
    if nc_file.suffix.lower() in (".hdf", ".hdf4"):
        return "L2"

    try:
        ds = xr.open_dataset(nc_file)
    except Exception:
        ds = None
    if ds is not None:
        try:
            latn = _root_latname(ds)
            if latn is not None:
                # 1D 單調 lat/lon = 規則網格 = L3;2D = swath = L2
                return "L3" if ds[latn].ndim == 1 else "L2"
            if {"scanline", "ground_pixel"} & set(ds.dims):
                return "L2"
        finally:
            ds.close()

    # S5P L2:root 沒有 lat/lon,資料在 PRODUCT group
    try:
        g = xr.open_dataset(nc_file, group="PRODUCT")
        g.close()
        return "L2"
    except Exception:
        pass

    # GEMS L2:root 空的,座標在 "Geolocation Fields" group(Latitude/Longitude 2D)。
    # ⚠️ 這條沒有時 GEMS 走 L3Pipeline 會在這裡 raise;collocate 那條路不經 build_field
    # 所以先前上千天都沒踩到。1D 就當已格網化的 L3(目前沒有這種 GEMS 檔,留個對稱)。
    try:
        g = xr.open_dataset(nc_file, group="Geolocation Fields")
        try:
            names = set(g.variables) | set(g.coords)
            has = {"Latitude", "Longitude"} <= names
            nd = int(g["Latitude"].ndim) if has else 0
        finally:
            g.close()
        if has:
            return "L2" if nd >= 2 else "L3"
    except Exception:
        pass

    raise ValueError(f"無法判斷 level(找不到 lat/lon 結構):{nc_file}")


def is_l3(nc_file: str | Path) -> bool:
    return detect_level(nc_file) == "L3"
