"""L3Writer:GriddedField → CF NetCDF (time, latitude, longitude) + 圖。

輸出格式與舊 SentinelProcessor 一致(同 dataset_name 變數),額外帶 count(覆蓋),
故可直接沿用 src.visualization.plot_nc.plot_global_var。
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import xarray as xr

from src.processing.l3.granule import GriddedField
from src.visualization.plot_nc import plot_global_var


class L3Writer:
    def to_dataset(self, gf: GriddedField) -> xr.Dataset:
        var = gf.product.dataset_name
        return xr.Dataset(
            {
                var: (["time", "latitude", "longitude"], gf.value[np.newaxis, :, :]),
                "count": (["time", "latitude", "longitude"], gf.count[np.newaxis, :, :]),
            },
            coords={"time": [gf.time], "latitude": gf.grid.lat, "longitude": gf.grid.lon},
            attrs={
                "units": gf.product.units,
                "description": gf.product.title,
                "processing_method": gf.method,
                "source": gf.source,
                "resolution": list(gf.grid.resolution),
            },
        )

    def write_nc(self, gf: GriddedField, out_path: str | Path) -> Path:
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        self.to_dataset(gf).to_netcdf(out_path)
        return out_path

    def write_figure(self, gf: GriddedField, out_nc: str | Path, fig_path: str | Path) -> None:
        Path(fig_path).parent.mkdir(parents=True, exist_ok=True)
        plot_global_var(dataset=str(out_nc), product_params=gf.product,
                        savefig_path=str(fig_path), map_scale="Taiwan", mark_stations=None)
