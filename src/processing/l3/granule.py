"""L3 pipeline 的 canonical 資料契約(source 無關)。

- GridSpec   : 目標網格的唯一真相(km 解析度 + bounds → GridFrame → 中心/邊界/HARP 參數)
- GranuleL2  : 一次過境的 swath 表示(中心 + 可選 footprint 角點 + QA)
- GriddedField: regrid 結果(value + 加權 count + 來源 metadata)
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from src.processing.grid_frame import GridFrame
from src.config.catalog import ProductConfig


@dataclass
class GridSpec:
    """目標網格的唯一真相。

    從 km 解析度(+ 可選 bounds)建 GridFrame,導出 cell 中心、bin 邊界,
    以及離線 HARP oracle 用的 bin_spatial 參數字串。三個 source 共用同一 GridSpec
    → 輸出逐格對齊,跨衛星疊圖/時間聚合才落在同一網格。
    """

    resolution: tuple[float, float]                               # (km_x, km_y)
    bounds: tuple[float, float, float, float] | None = None       # lon_min, lon_max, lat_min, lat_max

    def __post_init__(self):
        self._gf = GridFrame(self.resolution, bounds=self.bounds) if self.bounds else GridFrame(self.resolution)

    @property
    def lat(self) -> np.ndarray:
        return self._gf.lat

    @property
    def lon(self) -> np.ndarray:
        return self._gf.lon

    @staticmethod
    def _edges(c: np.ndarray) -> np.ndarray:
        step = np.diff(c).mean()
        return np.concatenate([[c[0] - step / 2], c[:-1] + np.diff(c) / 2, [c[-1] + step / 2]])

    @property
    def lat_edges(self) -> np.ndarray:
        return self._edges(self.lat)

    @property
    def lon_edges(self) -> np.ndarray:
        return self._edges(self.lon)

    def crop_mask(self, bounds: tuple[float, float, float, float]) -> tuple[np.ndarray, np.ndarray]:
        """回傳 (lat_mask, lon_mask) 把網格裁到 bounds=(lon_min,lon_max,lat_min,lat_max)。"""
        lon_min, lon_max, lat_min, lat_max = bounds
        return ((self.lat >= lat_min) & (self.lat <= lat_max),
                (self.lon >= lon_min) & (self.lon <= lon_max))

    def harp_bin_spatial(self) -> str:
        """離線 HARP oracle 用的 bin_spatial 參數(edge 數 = cell+1,.12g 精度避免漂移)。"""
        lat, lon = self.lat, self.lon
        s = float(np.diff(lat).mean())
        s2 = float(np.diff(lon).mean())
        return (f"bin_spatial({len(lat) + 1},{lat[0] - s / 2:.12g},{s:.12g},"
                f"{len(lon) + 1},{lon[0] - s2 / 2:.12g},{s2:.12g})")


@dataclass
class GranuleL2:
    """一次過境的 canonical swath 表示。

    lon/lat/values 皆為 2D (scanline, ground_pixel) 像元中心。
    lon_corners/lat_corners 為可選的 (n+1, m+1) footprint 角點;若 None,
    regridder 會從中心推導(中心→角誤差 ~0.1% 像元半徑,已驗證)。
    """

    values: np.ndarray
    lon: np.ndarray
    lat: np.ndarray
    time: np.datetime64
    product: ProductConfig
    qa: np.ndarray | None = None
    lon_corners: np.ndarray | None = None
    lat_corners: np.ndarray | None = None
    source: str = ""
    file_name: str = ""


@dataclass
class GriddedField:
    """regrid 結果:固定網格上的 value + 加權 count。"""

    value: np.ndarray          # (nlat, nlon)
    count: np.ndarray          # (nlat, nlon) 加權計數(覆蓋指標)
    grid: GridSpec
    product: ProductConfig
    time: np.datetime64
    source: str = ""
    file_name: str = ""
    method: str = ""
