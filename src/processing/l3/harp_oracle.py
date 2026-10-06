"""HARP oracle:用 HARP 的 ``bin_spatial`` 產生一份獨立參考網格,用來驗證自建的
超取樣 binning。

**HARP 不在生產路徑上** —— 它只是離線的第二意見。`SupersampleBinRegridder` 已對多軌
六種 S5P 產品驗證過(NO2/O3/CO/CH4 r ≥ 0.99、SO2/HCHO ≥ 0.975,見 tests/test_l3.py),生產一律走純 Python 那條;這支存在的意義是
「日後改動 regridder 時,還能重新跟一個外部標準對答案」。

需要 HARP CLI(`HARPCONVERT` 環境變數,或 PATH 上的 `harpconvert`),沒裝就回 None,
呼叫端自行跳過 —— 不要讓沒裝 HARP 變成錯誤。

網格化本身(`corners_from_centers` / `supersample`)只在 `regridder.py`,這裡只負責呼叫 HARP。
"""
from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import numpy as np

from src.processing.l3.granule import GridSpec


def _harp_bin() -> str | None:
    """HARP CLI 路徑:``HARPCONVERT`` 環境變數優先,否則找 PATH 上的 ``harpconvert``。

    呼叫時才查(不在 import 時決定),這樣 .env 晚一點載入也吃得到。
    """
    return os.environ.get("HARPCONVERT") or shutil.which("harpconvert")


#: S5P 產品碼 → (HARP 變數名, HARP validity 變數名)。
#: validity>50 等價於使用者慣用的 qa_value>=0.5(已確認一致)。
HARP_VARS = {
    "NO2___": ("tropospheric_NO2_column_number_density",
               "tropospheric_NO2_column_number_density_validity"),
    "HCHO__": ("tropospheric_HCHO_column_number_density",
               "tropospheric_HCHO_column_number_density_validity"),
    "O3____": ("O3_column_number_density",
               "O3_column_number_density_validity"),
    "SO2___": ("SO2_column_number_density",
               "SO2_column_number_density_validity"),
    "CO____": ("CO_column_number_density",
               "CO_column_number_density_validity"),
    # HARP 的 CH4 ingestion 預設讀「未做偏差修正」的 methane_mixing_ratio(ingestion option
    # ch4=bias_corrected 才換),正好就是 catalog 的 dataset_name —— 比的是同一個量。
    "CH4___": ("CH4_column_volume_mixing_ratio_dry_air",
               "CH4_column_volume_mixing_ratio_dry_air_validity"),
}


def harp_available() -> bool:
    """HARP CLI 是否可用。"""
    b = _harp_bin()
    return b is not None and Path(b).exists()


def harp_oracle(raw_nc: str | Path, product: str, grid: GridSpec,
                *, validity: int = 50, timeout: int = 300) -> np.ndarray | None:
    """跑 ``harpconvert`` 把一個 L2 granule 網格化到 ``grid``,回傳 2D 陣列。

    HARP 不可用 / 產品不支援 / 該軌無有效資料 → 回 ``None``(不 raise)。

    ⚠️ ``bin_spatial`` 參數必須用 ``.12g`` 精度輸出,否則會有 ~2e-5° 的網格漂移 ——
    這由 :meth:`GridSpec.harp_bin_spatial` 統一處理,不要在呼叫端自己拼字串。
    """
    if not harp_available() or product not in HARP_VARS:
        return None
    hvar, vvar = HARP_VARS[product]
    ops = f"{vvar}>{validity};{grid.harp_bin_spatial()};keep({hvar})"

    with tempfile.TemporaryDirectory() as td:
        out = str(Path(td) / "oracle.nc")
        try:
            r = subprocess.run([str(_harp_bin()), "-a", ops, str(raw_nc), out],
                               capture_output=True, text=True, timeout=timeout)
        except (OSError, subprocess.TimeoutExpired):
            return None
        if r.returncode != 0 or not Path(out).exists():
            return None
        import xarray as xr
        try:
            with xr.open_dataset(out) as ds:
                if hvar not in ds:
                    return None
                return np.asarray(ds[hvar].squeeze().values, dtype="float64")
        except Exception:
            return None
