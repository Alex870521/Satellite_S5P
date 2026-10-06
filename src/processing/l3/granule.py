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

    兩種建法,擇一:

    * **km 模式**(``resolution``):走 GridFrame,把公里換算成度數。沿用舊行為,
      但換算依緯度而變 → 度數不是整數,格數也不好預測。適合「跟著儀器原生足跡走」。
    * **度數模式**(``deg``):直接用 ``np.arange`` 產生精確的度數網格。統一解析度時
      **必須**用這個 —— 目標網格要能逐格重現、且跨 source/跨年完全一致,km 換算的
      浮點漂移會讓格數在 201/202 之間跳。

    三個 source 共用同一 GridSpec → 輸出逐格對齊,跨衛星疊圖/時間聚合才落在同一網格。
    """

    resolution: tuple[float, float] | None = None                 # (km_x, km_y)
    bounds: tuple[float, float, float, float] | None = None       # lon_min, lon_max, lat_min, lat_max
    deg: tuple[float, float] | None = None                        # (deg_lon, deg_lat) 精確度數網格

    # 度數↔公里換算(與 GridFrame 同一組常數,只用來把 deg 模式的等效 km 填進 metadata)
    _LAT_KM_PER_DEG = 111.32
    _EARTH_RADIUS_KM = 6371.0

    def __post_init__(self):
        if self.deg is not None:
            if self.bounds is None:
                raise ValueError("deg 模式必須同時給 bounds=(lon_min, lon_max, lat_min, lat_max)")
            lon_min, lon_max, lat_min, lat_max = self.bounds
            dlon, dlat = self.deg
            # +1e-9 讓終點含進來(等同 config.py 的 arange(..., stop + 1e-6, res) 慣例);
            # round 到 6 位小數消掉 arange 的浮點尾巴,確保逐格值可重現。
            # 度數模式存精確陣列;公里模式改走 GridFrame,這兩個是 None
            self._lon_arr: np.ndarray | None = np.round(np.arange(lon_min, lon_max + 1e-9, dlon), 6)
            self._lat_arr: np.ndarray | None = np.round(np.arange(lat_min, lat_max + 1e-9, dlat), 6)
            self._gf = None
            if self.resolution is None:                 # 補等效 km,供 writer metadata
                center_lat = (lat_min + lat_max) / 2
                km_per_deg_lon = (self._EARTH_RADIUS_KM * np.cos(np.radians(center_lat))
                                  * 2 * np.pi / 360)
                self.resolution = (round(float(dlon * km_per_deg_lon), 4),
                                   round(float(dlat * self._LAT_KM_PER_DEG), 4))
        else:
            if self.resolution is None:
                raise ValueError("必須給 resolution(km 模式)或 deg(度數模式)")
            self._gf = GridFrame(self.resolution, bounds=self.bounds) if self.bounds else GridFrame(self.resolution)
            self._lat_arr = self._lon_arr = None

    @classmethod
    def from_degrees(cls, deg: float | tuple[float, float],
                     bounds: tuple[float, float, float, float]) -> "GridSpec":
        """精確度數網格。``deg`` 給單一數字 = 經緯同解析度。"""
        d = (deg, deg) if isinstance(deg, (int, float)) else deg
        return cls(bounds=bounds, deg=d)

    @property
    def lat(self) -> np.ndarray:
        if self._gf is not None:
            return self._gf.lat
        assert self._lat_arr is not None      # 度數模式由 __post_init__ 保證
        return self._lat_arr

    @property
    def lon(self) -> np.ndarray:
        if self._gf is not None:
            return self._gf.lon
        assert self._lon_arr is not None
        return self._lon_arr

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
    scan_time 為可選的逐掃描線時間 (n,);有給時 pipeline 會把 ``time`` 改成
    「落在目標網格內那幾條掃描線的平均時刻」= 真正的過境時間(S5P 的 ``time`` 只是
    當天 00:00 UTC 的參考時間,逐軌輸出需要這個)。
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
    scan_time: np.ndarray | None = None


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
