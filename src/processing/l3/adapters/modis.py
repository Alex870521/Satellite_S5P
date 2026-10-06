"""MODIS adapter:MODIS AOD → GranuleL2。

吃兩種輸入,都吐同一個 GranuleL2:

* **`.nc`**(``MODISProcessor.hdf4_to_netcdf`` 的 1:1 無損轉檔)—— 一般路徑,
  不需要 pyhdf,全平台可用。
* **`.hdf`**(原始 HDF4)—— 直接讀,省下中繼檔(整年 AOD 轉檔約 14GB)。
  HDF4 仍然只透過 ``MODISProcessor`` 的抽取方法碰,**沒有另開第二個 HDF4 入口**,
  所以架構上的「單一 ingest 入口」不變;代價是這條路徑需要 pyhdf(``[ingest]`` extra,
  py3.12/3.13)。

MODIS 沒有 QA 欄位進到這層(``_extract_*`` 已套用 scale/fill 並濾掉無效值),
所以 ``qa=None`` → regridder 一律當權重 1。footprint 角點也一律從中心推導。
"""
from __future__ import annotations

import re
import warnings
from pathlib import Path
from typing import Iterable, Iterator

import numpy as np
import xarray as xr

from src.config.catalog import PRODUCT_CONFIGS
from src.processing.l3.granule import GranuleL2

# MCD19A2.A2023001.h28v06...  /  MOD04_L2.A2023001.0250...
_DATE_RE = re.compile(r"\.A(\d{4})(\d{3})\.(?:(\d{2})(\d{2})\.)?")


def _date_from_name(name: str) -> np.datetime64 | None:
    """MODIS 檔名的 A{YYYY}{DOY}[.HHMM] → datetime64。取不到回 None。

    MOD04/MYD04 是 5 分鐘 granule,檔名帶 UTC 起始時刻(逐軌輸出要用);
    MCD19A2 是已合併多軌的逐日 tile,只有日期。
    """
    m = _DATE_RE.search(name)
    if not m:
        return None
    year, doy = int(m.group(1)), int(m.group(2))
    t = np.datetime64(f"{year}-01-01", "ns") + np.timedelta64(doy - 1, "D")
    if m.group(3) is not None:
        t = t + np.timedelta64(int(m.group(3)), "h") + np.timedelta64(int(m.group(4)), "m")
    return t


class MODISAdapter:
    source = "MODIS"

    def __init__(self, file_type: str = "MCD19A2"):
        self.file_type = file_type
        self.product = PRODUCT_CONFIGS[file_type]

    # ------------------------------------------------------------------ #
    def read(self, nc_file: str | Path) -> GranuleL2 | None:
        path = Path(nc_file)
        if path.suffix.lower() == ".hdf":
            arrays = self._read_hdf(path)
        else:
            arrays = self._read_nc(path)
        if arrays is None:
            return None
        val, lat, lon = arrays

        time = _date_from_name(path.name)
        if time is None:
            return None

        # 有效值都沒有就不必往下走(regridder 也會回全 NaN,早退比較省)
        if not np.isfinite(val).any():
            return None

        return GranuleL2(
            values=np.asarray(val, dtype="float64"),
            lon=np.asarray(lon, dtype="float64"),
            lat=np.asarray(lat, dtype="float64"),
            time=time, product=self.product, qa=None,
            source=self.source, file_name=path.name,
        )

    # ------------------------------------------------------------------ #
    def _read_nc(self, path: Path):
        """讀 hdf4_to_netcdf 產出的 swath nc(aod + 2D latitude/longitude)。"""
        ds = xr.open_dataset(path)
        try:
            var = self.product.dataset_name
            if var not in ds:
                return None
            val = ds[var].values
            lat = ds["latitude"].values
            lon = ds["longitude"].values
        finally:
            ds.close()
        if val.ndim != 2 or lat.shape != val.shape or lon.shape != val.shape:
            return None
        return val, lat, lon

    def _read_hdf(self, path: Path):
        """直接讀原始 HDF4,復用 MODISProcessor 的抽取方法(唯一的 HDF4 入口)。"""
        import logging

        from src.processing.modis_processor import MODISProcessor

        proc = MODISProcessor()
        proc.file_type = self.file_type
        # processor 平常由 hub 注入 logger;這裡自己給一個,否則錯誤路徑會 AttributeError
        proc.logger = proc.logger or logging.getLogger(__name__)
        hdf_obj = proc._open_with_pyhdf(path)
        if not hdf_obj:
            return None
        try:
            datasets = hdf_obj.datasets()
            if proc._is_mcd19a2_file(path.name):
                # keep_orbits=True:MCD19A2 一天多次過境,只取第一層會丟掉約 165% 的
                # 有效點,還會讓「第一層剛好全空」的日子整天被誤判為無資料。
                val, lat, lon = proc._extract_mcd19a2_data(hdf_obj, datasets, path.name,
                                                           keep_orbits=True)
            else:
                val, lat, lon = proc._extract_mod04_data(hdf_obj, datasets)
        finally:
            proc._close_hdf_file(hdf_obj)
        if val is None or lat is None or lon is None:
            return None
        val, lat, lon = np.asarray(val), np.asarray(lat), np.asarray(lon)
        # (orbit, y, x) → 沿軌道對有效值取平均(全 NaN 的格仍是 NaN)
        if val.ndim == 3:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", RuntimeWarning)   # all-NaN slice
                val = np.nanmean(val, axis=0)
        if val.ndim != 2 or lat.shape != val.shape or lon.shape != val.shape:
            return None
        return val, lat, lon

    # ------------------------------------------------------------------ #
    def iter_granules(self, files: Iterable[str | Path]) -> Iterator[GranuleL2]:
        for f in files:
            g = self.read(f)
            if g is not None:
                yield g
