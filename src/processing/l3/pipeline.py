"""L3Pipeline:Adapter → Regridder → Writer 的編排器。

階段一(現況):逐軌格網化。process_file/process_files 把每個 granule regrid 成
一張 GriddedField,寫 nc + 圖。
階段二(預留):L3Accumulator 在固定網格上做時間聚合(daily/monthly 加權平均)。
"""
from __future__ import annotations

from pathlib import Path
from typing import Iterable

import numpy as np

from src.processing.l3.granule import GranuleL2, GridSpec, GriddedField
from src.processing.l3.writer import L3Writer


class L3Pipeline:
    def __init__(self, adapter, regridder, grid: GridSpec, writer: L3Writer | None = None):
        self.adapter = adapter
        self.regridder = regridder
        self.grid = grid
        self.writer = writer or L3Writer()

    def regrid_granule(self, g: GranuleL2) -> GriddedField:
        return self.regridder.regrid(g, self.grid)

    def process_file(self, nc_file: str | Path,
                     out_nc: str | Path | None = None,
                     fig_path: str | Path | None = None) -> GriddedField | None:
        g = self.adapter.read(nc_file)
        if g is None:
            return None
        gf = self.regrid_granule(g)
        if out_nc is not None:
            self.writer.write_nc(gf, out_nc)
            if fig_path is not None:
                self.writer.write_figure(gf, out_nc, fig_path)
        return gf

    def process_files(self, files: Iterable[str | Path], out_dir: str | Path | None = None):
        results = []
        for f in files:
            f = Path(f)
            out_nc = (Path(out_dir) / f.name) if out_dir else None
            results.append(self.process_file(f, out_nc=out_nc))
        return results


class L3Accumulator:
    """階段二:固定網格上的時間聚合(running 加權平均)。

    因為每個 granule 經超取樣後已是整張網格(覆蓋外 NaN),聚合 = 跨 granule 的
    逐格加權平均(權重 = 該格 count)。輸出 value / count / std。
    """

    def __init__(self, grid: GridSpec):
        shape = (len(grid.lat), len(grid.lon))
        self.grid = grid
        self._wsum = np.zeros(shape)
        self._sum = np.zeros(shape)
        self._sqsum = np.zeros(shape)
        self._n = np.zeros(shape)

    def add(self, gf: GriddedField) -> None:
        v = gf.value
        w = np.where(np.isfinite(v), gf.count, 0.0)
        v0 = np.where(np.isfinite(v), v, 0.0)
        self._wsum += w
        self._sum += w * v0
        self._sqsum += w * v0 * v0
        self._n += (w > 0)

    def finalize(self) -> dict:
        with np.errstate(invalid="ignore", divide="ignore"):
            mean = self._sum / self._wsum
            var = self._sqsum / self._wsum - mean * mean
        mean[self._wsum == 0] = np.nan
        std = np.sqrt(np.clip(var, 0, None))
        std[self._wsum == 0] = np.nan
        return {"value": mean, "count": self._n, "std": std}
