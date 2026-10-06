"""把「一批 raw 檔 → 統一網格的時間序列 nc」包成一個可重用的函式。

``scripts/l3_regrid_year.py``(CLI)與各 hub 的 ``process_l3()``(API 委派)都走這裡,
避免兩邊各寫一份編排邏輯。
"""
from __future__ import annotations

import re
import time
from pathlib import Path
from typing import Iterable

import numpy as np

from src.processing.l3.granule import GridSpec
from src.processing.l3.pipeline import L3Pipeline
from src.processing.l3.regridder import SupersampleBinRegridder
from src.processing.l3.writer import L3Writer

DEFAULT_BOUNDS = (119.0, 123.0, 21.0, 26.0)

SHORT_NAME = {"NO2___": "no2", "O3____": "o3", "SO2___": "so2", "HCHO__": "hcho",
              "MCD19A2": "aod", "MOD04_L2": "aod", "MYD04_L2": "aod",
              # GEMS 用 catalog 的 adapter key;一個 NO2 檔含三個柱量,短名要分得開
              "GEMS_NO2": "no2_total", "GEMS_NO2_TROP": "no2_trop",
              "GEMS_NO2_STRAT": "no2_strat", "GEMS_O3T": "o3",
              "GEMS_HCHO": "hcho", "GEMS_SO2": "so2", "GEMS_AERAOD": "aod", "GEMS_UVI": "uvi"}

# GEMS 的 adapter key(catalog)→ 外接碟 raw 子目錄。S5P/MODIS 的 product 名本身就是目錄名,
# GEMS 不是(一個目錄 NO2 對應三個 key),所以要這張表;CLI 與任何要找 raw 的地方共用。
GEMS_RAW_DIR = {"GEMS_NO2": "NO2", "GEMS_NO2_TROP": "NO2", "GEMS_NO2_STRAT": "NO2",
                "GEMS_O3T": "O3T", "GEMS_HCHO": "HCHO", "GEMS_SO2": "SO2",
                "GEMS_AERAOD": "AERAOD", "GEMS_UVI": "UVI"}


_GEMS_SLOT = re.compile(r"_\d{8}_(\d{4})_")


def gems_slot(name: str) -> str | None:
    """GEMS 檔名的時槽(UTC HHMM),例 ``GK2_GEMS_L2_20220101_0445_NO2_…`` → ``"0445"``。"""
    m = _GEMS_SLOT.search(name)
    return m.group(1) if m else None


def make_adapter(source: str, product: str, **adapter_kwargs):
    """source 名 → adapter 實例。"""
    source = source.lower()
    if source in ("s5p", "sentinel5p"):
        from src.processing.l3.adapters import S5PAdapter
        return S5PAdapter(product)
    if source == "modis":
        from src.processing.l3.adapters import MODISAdapter
        return MODISAdapter(product)
    if source == "gems":
        from src.processing.l3.adapters import GEMSAdapter
        return GEMSAdapter(product, **adapter_kwargs)
    raise ValueError(f"未知 source: {source!r}")


def regrid_to_series(source: str, product: str, files: Iterable[str | Path],
                     out_path: str | Path, *,
                     deg: float = 0.02,
                     bounds: tuple[float, float, float, float] = DEFAULT_BOUNDS,
                     freq: str = "D", K: int = 4, qa: float = 0.5,
                     short_name: str | None = None,
                     extra_attrs: dict | None = None,
                     progress=None,
                     adapter_kwargs: dict | None = None,
                     tz_offset_hours: float = 0) -> dict:
    """一批 raw → 統一網格的 ``(time, lat, lon)`` nc。

    回傳統計 dict(``n_files``/``n_periods``/``n_skipped``/``mean_coverage``/``out``/``seconds``)。

    ``adapter_kwargs``:傳給 adapter 的品質篩選參數(GEMS:``cloud_max`` / ``rms_max``)。
    ``tz_offset_hours``:分窗用的時區位移(GEMS 用 8 = 台灣當地日期)。
    """
    files = [Path(f) for f in files]
    if not files:
        raise FileNotFoundError(f"{source}/{product}: 沒有可處理的 raw 檔")

    grid = GridSpec.from_degrees(deg, bounds)
    adapter = make_adapter(source, product, **(adapter_kwargs or {}))
    pipe = L3Pipeline(adapter, SupersampleBinRegridder(K=K, qa_threshold=qa),
                      grid, L3Writer())

    t0 = time.time()
    skipped = 0

    def _progress(f, gf):                       # 包一層:runner 自己數略過的檔,呼叫端回呼照常
        nonlocal skipped
        if gf is None:
            skipped += 1
        if progress:
            progress(f, gf)

    res = pipe.aggregate(files, freq=freq, progress=_progress, tz_offset_hours=tz_offset_hours)
    if not res:
        raise ValueError(f"{source}/{product}: 聚合結果為空(所有 granule 都無有效資料)")

    periods = [p for p, _ in res]
    results = [r for _, r in res]
    attrs = {"source": source, "product": product,
             "n_source_files": str(len(files)), "n_skipped": str(skipped),
             "qa_threshold": str(qa),
             "supersample_K": str(K), "freq": freq,
             "date_basis": f"UTC{tz_offset_hours:+g}h" if tz_offset_hours else "UTC"}
    if freq.lower() in ("granule", "g"):
        attrs["date_basis"] = "UTC"
        attrs["time_basis"] = ("granule overpass: mean time of the scanlines inside the grid "
                               "(S5P) / file-name time (GEMS, MOD04/MYD04)")
    for k, v in (adapter_kwargs or {}).items():
        attrs[k] = str(v)
    attrs.update(extra_attrs or {})
    out = L3Writer().write_series(periods, results, grid, adapter.product, out_path,
                                  short_name=short_name or SHORT_NAME.get(product),
                                  extra_attrs=attrs)
    cov = float(np.mean([np.isfinite(r["value"]).mean() for r in results]) * 100)
    return {"n_files": len(files), "n_periods": len(res), "n_skipped": skipped,
            "mean_coverage": cov,
            "out": out, "seconds": time.time() - t0}
