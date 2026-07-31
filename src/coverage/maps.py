"""逐格統計:對**時間**做 reduction,輸出**地圖**。

與這個套件的其他部分**軸向正交**:`engine.compute_coverage` 是對**空間**做 reduction
(區域內有效格 / 總格數)→ 得到時間序列;這裡是對**時間**做 reduction(每一格有幾天有觀測)
→ 得到地圖。兩者回答不同問題,所以是兩支模組而不是一支的參數。

- `annual_mean` — 每格的時間平均(氣候態場)
- `data_count`  — 每格有幾個時間步有效(觀測數)
- `coverage_map` — 每格的時間可用率 %(= data_count / 總時間步)

⚠️ **和 L3 檔裡的 `<var>_count` 不是同一件事**:那個是超取樣子點數(空間取樣密度),
這裡的 count 是**時間維度上有幾天有資料**。同一個字但兩種語意,別混用。

吃任何 `(time, lat, lon)` 的 nc —— 新的 L3 統一網格檔、舊的 merge 年檔都可以。
從 `wip_coverage/analyze_modis_aod.py` 收進來:原版把三個計算綁在一個帶 logging 的
analyzer class 上、且各產品的 vmin/vmax/cmap 硬寫在呼叫端的 config dict;這裡改成
純函式 + 從 `catalog` 取產品設定。
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import xarray as xr


def _open(src) -> xr.Dataset:
    return src if isinstance(src, xr.Dataset) else xr.open_dataset(src)


def _main_var(ds: xr.Dataset, var: str | None) -> str:
    """挑主變數:明示優先,否則取第一個非 _count/_std 的資料變數。"""
    if var is not None:
        return var
    for v in ds.data_vars:
        if not str(v).endswith(("_count", "_std")):
            return str(v)
    raise ValueError(f"找不到主變數(data_vars={list(ds.data_vars)})")


def per_cell_stats(src, var: str | None = None) -> dict:
    """一次算完三個逐格統計,回傳 dict of DataArray。

    ``src`` 可以是路徑或已開好的 Dataset。回傳
    ``{"mean", "count", "coverage", "n_time", "var"}``,
    其中 ``coverage`` 是 0–100 的百分比。
    """
    ds = _open(src)
    try:
        name = _main_var(ds, var)
        da = ds[name]
        if "time" not in da.dims:
            raise ValueError(f"{name} 沒有 time 維度,無法對時間 reduction")
        n_time = int(da.sizes["time"])
        mean = da.mean(dim="time", skipna=True)
        count = da.count(dim="time")
        coverage = (count / n_time) * 100 if n_time else count * np.nan
        return {"mean": mean.compute(), "count": count.compute(),
                "coverage": coverage.compute(), "n_time": n_time, "var": name}
    finally:
        if not isinstance(src, xr.Dataset):
            ds.close()


def plot_cell_stats(src, *, var: str | None = None, product: str | None = None,
                    output=None, title: str | None = None,
                    cmap: str | None = None, vmin=None, vmax=None,
                    dpi: int = 600):
    """三聯圖:年均場 / 觀測數 / 逐格時間覆蓋率。

    ``product`` 給 catalog 的產品碼(如 ``"NO2___"``/``"MCD19A2"``)時,
    色階與值域沿用該產品設定;也可用 ``cmap``/``vmin``/``vmax`` 直接覆寫。
    """
    import matplotlib.pyplot as plt
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature

    st = per_cell_stats(src, var)
    mean, count, cov = st["mean"], st["count"], st["coverage"]

    if product is not None:
        from src.config.catalog import PRODUCT_CONFIGS
        cfg = PRODUCT_CONFIGS.get(product)
        if cfg is not None:
            cmap = cmap or cfg.cmap
            vmin = cfg.vmin if vmin is None else vmin
            vmax = cfg.vmax if vmax is None else vmax
    cmap = cmap or "YlOrRd"
    if vmax is None:
        finite = mean.values[np.isfinite(mean.values)]
        vmax = float(np.nanpercentile(finite, 98)) if finite.size else 1.0

    yname = "lat" if "lat" in mean.dims else "latitude"
    xname = "lon" if "lon" in mean.dims else "longitude"
    proj = ccrs.PlateCarree()
    fig, axes = plt.subplots(1, 3, figsize=(16.5, 5.2),
                             subplot_kw={"projection": proj})
    panels = [
        (mean, f"Temporal mean ({st['var']})", cmap, vmin, vmax),
        (count, f"Valid time steps (of {st['n_time']})", "viridis", 0, None),
        (cov, "Temporal coverage (%)", "viridis", 0, 100),
    ]
    for ax, (fld, label, cm, lo, hi) in zip(axes, panels):
        ax.add_feature(cfeature.COASTLINE, linewidth=0.5)
        im = ax.pcolormesh(fld[xname], fld[yname], fld.values, cmap=cm,
                           vmin=lo, vmax=hi, shading="auto", transform=proj)
        ax.set_title(label, fontsize=14)
        fig.colorbar(im, ax=ax, shrink=0.75)

    fig.suptitle(title or f"{st['var']} — per-cell statistics over "
                          f"{st['n_time']} time steps",
                 fontsize=16, fontweight="bold")
    if output is not None:
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output, dpi=dpi, bbox_inches="tight", facecolor="white")
        plt.close(fig)
    return fig
