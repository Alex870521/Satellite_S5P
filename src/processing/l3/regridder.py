"""Regridder:L2 granule → 固定網格 GriddedField。

- SupersampleBinRegridder: footprint 超取樣 binning(physical oversampling)。三 source
  共用的量化預設。純 Python(scipy),補滿覆蓋 + 面積加權自然浮現。多軌×多氣體
  驗證 vs HARP oracle r min 0.996 / mean 0.999。取代 HARP runtime。
- RbfRegridder: 可選平滑/看圖 mode,沿用既有 DataInterpolator。
"""
from __future__ import annotations

from typing import Protocol

import numpy as np
from scipy.stats import binned_statistic_2d

from src.processing.interpolators import DataInterpolator
from src.processing.l3.granule import GranuleL2, GridSpec, GriddedField


def corners_from_centers(a: np.ndarray) -> np.ndarray:
    """2D cell 中心 (n, m) → (n+1, m+1) 角點。

    內部角點 = 周圍 4 個中心平均;邊界線性外插。誤差 ~0.1% 像元半徑(已對 S5P
    真 latitude_bounds 驗證),供無 footprint bounds 的 GEMS/MODIS 使用。
    """
    p = np.full((a.shape[0] + 1, a.shape[1] + 1), np.nan)
    p[1:-1, 1:-1] = 0.25 * (a[:-1, :-1] + a[1:, :-1] + a[:-1, 1:] + a[1:, 1:])
    p[0, 1:-1] = 2 * p[1, 1:-1] - p[2, 1:-1]
    p[-1, 1:-1] = 2 * p[-2, 1:-1] - p[-3, 1:-1]
    p[:, 0] = 2 * p[:, 1] - p[:, 2]
    p[:, -1] = 2 * p[:, -2] - p[:, -3]
    return p


class Regridder(Protocol):
    def regrid(self, g: GranuleL2, grid: GridSpec) -> GriddedField: ...


class SupersampleBinRegridder:
    """Footprint 超取樣 binning。

    每個有效像元:取 footprint 4 角點(granule 自帶或從中心推)→ bilinear 灑 K×K
    子點(權重 qa/K²)→ 全部子點丟進 binned_statistic_2d 加權平均。大像元的子點
    散落到 footprint 蓋到的所有 cell → 補滿覆蓋;子點落格比例 ≈ 面積佔比 → 面積加權。
    """

    def __init__(self, K: int = 4, qa_threshold: float = 0.5):
        self.K = K
        self.qa_threshold = qa_threshold

    def regrid(self, g: GranuleL2, grid: GridSpec) -> GriddedField:
        lat, lon, val = g.lat, g.lon, g.values
        qa = g.qa if g.qa is not None else np.ones_like(val, dtype=float)
        lon_e, lat_e = grid.lon_edges, grid.lat_edges
        nlat, nlon = len(grid.lat), len(grid.lon)

        latp = g.lat_corners if g.lat_corners is not None else corners_from_centers(lat)
        lonp = g.lon_corners if g.lon_corners is not None else corners_from_centers(lon)

        # 限縮到網格範圍 + QA 門檻 + 有效值(加速,避免全 swath 灑子點)
        in_box = (lon >= lon_e[0]) & (lon <= lon_e[-1]) & (lat >= lat_e[0]) & (lat <= lat_e[-1])
        mask = in_box & (qa >= self.qa_threshold) & np.isfinite(val) & np.isfinite(lat) & np.isfinite(lon)
        ii = np.where(mask)

        if ii[0].size == 0:
            return GriddedField(np.full((nlat, nlon), np.nan), np.zeros((nlat, nlon)),
                                grid, g.product, g.time, g.source, g.file_name, "supersample")

        px, py, w, z = self._subpoints(lonp, latp, val[ii], qa[ii], ii)
        num = binned_statistic_2d(px, py, z * w, "sum", bins=[lon_e, lat_e])[0].T
        den = binned_statistic_2d(px, py, w, "sum", bins=[lon_e, lat_e])[0].T
        cnt = binned_statistic_2d(px, py, w, "count", bins=[lon_e, lat_e])[0].T
        with np.errstate(invalid="ignore"):
            value = num / den
        value[den == 0] = np.nan
        return GriddedField(value, cnt, grid, g.product, g.time, g.source, g.file_name, "supersample")

    def _subpoints(self, lonp, latp, val, qa, ii):
        """每個像元 footprint 內 bilinear 灑 K×K 子點。回傳 (px, py, w, z) 攤平。"""
        si, gj = ii
        K = self.K
        # 4 角點:A=(i,j) B=(i+1,j) C=(i+1,j+1) D=(i,j+1)(繞行 quad)
        ax, bx, cx, dx = lonp[si, gj], lonp[si + 1, gj], lonp[si + 1, gj + 1], lonp[si, gj + 1]
        ay, by, cy, dy = latp[si, gj], latp[si + 1, gj], latp[si + 1, gj + 1], latp[si, gj + 1]
        u = (np.arange(K) + 0.5) / K
        s, t = np.meshgrid(u, u)
        s, t = s.ravel(), t.ravel()

        def bilinear(a, b, c, d):
            # P(s,t) = (1-s)(1-t)A + s(1-t)B + stC + (1-s)tD
            return ((1 - s)[None, :] * (1 - t)[None, :] * a[:, None]
                    + s[None, :] * (1 - t)[None, :] * b[:, None]
                    + s[None, :] * t[None, :] * c[:, None]
                    + (1 - s)[None, :] * t[None, :] * d[:, None]).ravel()

        px = bilinear(ax, bx, cx, dx)
        py = bilinear(ay, by, cy, dy)
        w = np.repeat(qa, K * K) / (K * K)
        z = np.repeat(val, K * K)
        ok = np.isfinite(px) & np.isfinite(py) & np.isfinite(w) & np.isfinite(z)
        return px[ok], py[ok], w[ok], z[ok]


class RbfRegridder:
    """可選平滑/看圖 mode:沿用既有 DataInterpolator(rbf/griddata/kdtree)。"""

    def __init__(self, method: str = "rbf", max_distance: float = 0.1, rbf_function: str = "thin_plate"):
        self.method = method
        self.max_distance = max_distance
        self.rbf_function = rbf_function

    def regrid(self, g: GranuleL2, grid: GridSpec) -> GriddedField:
        lon_grid, lat_grid = np.meshgrid(grid.lon, grid.lat)
        qa = g.qa if g.qa is not None else np.ones_like(g.values, dtype=float)
        val = np.where(qa >= 0.5, g.values, np.nan)
        value = DataInterpolator.interpolate(
            g.lon, g.lat, val, lon_grid, lat_grid,
            method=self.method, max_distance=self.max_distance, rbf_function=self.rbf_function,
        )
        cnt = np.isfinite(value).astype(float)
        return GriddedField(value, cnt, grid, g.product, g.time, g.source, g.file_name, self.method)
