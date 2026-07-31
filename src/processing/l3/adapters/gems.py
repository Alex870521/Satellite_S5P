"""GEMS adapter:GEMS L2 (Data Fields / Geolocation Fields) → GranuleL2。

與 ``GEMSProcessor.extract_data`` 讀同樣的東西、套同樣的 QC,但**不攤平**:
超取樣 regridder 需要 2D (spatial, image) 才能從中心推 footprint 角點,所以這裡把
QC 結果轉成 ``qa`` 權重(通過=1、不通過=0)交給 regridder 的 ``qa_threshold`` 濾掉,
而不是先把點挑出來。效果等價,但保留了幾何。

QC 預設與 GEMSProcessor 一致:``FinalAlgorithmFlags == 0``(0=best,是 bitfield)+ 去負。
AERAOD 是三波長 (nwavel, spatial, image) 且 flags 不適用 → 用 ``band`` 選波段、
並自動關掉 flag 判斷(見 [[gems-openapi-integration]] 的坑)。
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable, Iterator

import numpy as np
import xarray as xr

from src.config.catalog import PRODUCT_CONFIGS
from src.processing.l3.granule import GranuleL2

DATA_GROUP = "Data Fields"
GEO_GROUP = "Geolocation Fields"

# GK2_GEMS_L2_20230515_0045_NO2_..._.nc
_DT_RE = re.compile(r"_(\d{8})_(\d{4})_")


def _time_from_name(name: str) -> np.datetime64 | None:
    m = _DT_RE.search(name)
    if not m:
        return None
    d, hm = m.group(1), m.group(2)
    return np.datetime64(f"{d[:4]}-{d[4:6]}-{d[6:8]}T{hm[:2]}:{hm[2:]}", "ns")


class GEMSAdapter:
    source = "GEMS"

    def __init__(self, file_type: str = "GEMS_NO2", *,
                 qc_flag_var: str | None = "FinalAlgorithmFlags",
                 qc_good_value: int = 0,
                 mask_negative: bool = True,
                 cloud_max: float | None = None,
                 band: int | None = None):
        self.file_type = file_type
        self.product = PRODUCT_CONFIGS[file_type]
        self.qc_flag_var = qc_flag_var
        self.qc_good_value = qc_good_value
        self.mask_negative = mask_negative
        self.cloud_max = cloud_max
        self.band = band

    def read(self, nc_file: str | Path) -> GranuleL2 | None:
        path = Path(nc_file)
        try:
            data = xr.open_dataset(path, group=DATA_GROUP, engine="netcdf4", mask_and_scale=True)
            geo = xr.open_dataset(path, group=GEO_GROUP, engine="netcdf4", mask_and_scale=True)
        except (OSError, KeyError):
            return None
        try:
            var_name = self.product.dataset_name
            if var_name not in data:
                return None
            val = np.asarray(data[var_name].values, dtype="float64")
            lat = np.asarray(geo["Latitude"].values, dtype="float64")
            lon = np.asarray(geo["Longitude"].values, dtype="float64")

            if val.ndim == 3:                      # AERAOD: (nwavel, spatial, image)
                val = val[self.band if self.band is not None else 0]
            if val.ndim != 2 or lat.shape != val.shape or lon.shape != val.shape:
                return None

            ok = np.isfinite(val) & np.isfinite(lat) & np.isfinite(lon)
            if self.mask_negative:
                ok &= val > 0
            if self.qc_flag_var and self.qc_flag_var in data:
                flags = np.asarray(data[self.qc_flag_var].values)
                if flags.shape == val.shape:       # 三波長產品的 flags 是 2D 且不適用 → 跳過
                    ok &= flags == self.qc_good_value
            if self.cloud_max is not None and "CloudFraction" in data:
                cf = np.asarray(data["CloudFraction"].values, dtype="float64")
                if cf.shape == val.shape:
                    ok &= np.isfinite(cf) & (cf <= self.cloud_max)

            if not ok.any():
                return None

            time = _time_from_name(path.name)
            if time is None:
                return None

            # QC → qa 權重:regridder 的 qa_threshold=0.5 會濾掉 0 的像元
            qa = ok.astype("float64")
            return GranuleL2(
                values=np.where(ok, val, np.nan), lon=lon, lat=lat, time=time,
                product=self.product, qa=qa,
                source=self.source, file_name=path.name,
            )
        finally:
            data.close()
            geo.close()

    def iter_granules(self, files: Iterable[str | Path]) -> Iterator[GranuleL2]:
        for f in files:
            g = self.read(f)
            if g is not None:
                yield g
