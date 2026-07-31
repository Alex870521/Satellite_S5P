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

    # ------------------------------------------------------------------ #
    def series_to_dataset(self, periods, results, grid, product, *,
                          short_name: str | None = None,
                          extra_attrs: dict | None = None) -> xr.Dataset:
        """階段二聚合結果 → 單一 (time, lat, lon) Dataset。

        ``periods``/``results`` = ``L3Pipeline.aggregate()`` 回傳的兩欄。
        座標刻意用 **lat/lon 短名**、變數用 ``short_name``(預設沿用 product 的
        dataset_name),這樣模型端 data.py 可以直接讀,不必再改名 —— 這是舊管線
        每次 merge 完都得手動 rename 的那一步,在這裡一次做對。
        """
        var = short_name or product.dataset_name
        value = np.stack([r["value"] for r in results]).astype("float32")
        count = np.stack([r["count"] for r in results]).astype("float32")
        std = np.stack([r["std"] for r in results]).astype("float32")
        time = np.array([np.datetime64(p, "ns") for p in periods])
        attrs = {
            "units": product.units,
            "description": product.title,
            "processing_method": "supersample_bin",
            "grid_deg": list(grid.deg) if grid.deg else "",
            "grid_shape": [len(grid.lat), len(grid.lon)],
            "resolution_km": list(grid.resolution),
            "produced_by": "src.processing.l3",
        }
        attrs.update(extra_attrs or {})
        return xr.Dataset(
            {var: (["time", "lat", "lon"], value),
             f"{var}_count": (["time", "lat", "lon"], count),
             f"{var}_std": (["time", "lat", "lon"], std)},
            coords={"time": time, "lat": grid.lat, "lon": grid.lon},
            attrs=attrs,
        )

    def write_series(self, periods, results, grid, product, out_path: str | Path, *,
                     short_name: str | None = None, compress: bool = True,
                     extra_attrs: dict | None = None) -> Path:
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        ds = self.series_to_dataset(periods, results, grid, product,
                                    short_name=short_name, extra_attrs=extra_attrs)
        enc = ({v: {"zlib": True, "complevel": 4} for v in ds.data_vars} if compress else None)
        ds.to_netcdf(out_path, encoding=enc)
        return out_path

    def write_figure(self, gf: GriddedField, out_nc: str | Path, fig_path: str | Path) -> None:
        Path(fig_path).parent.mkdir(parents=True, exist_ok=True)
        plot_global_var(dataset=str(out_nc), product_params=gf.product,
                        savefig_path=str(fig_path), map_scale="Taiwan", mark_stations=None)
