"""S5P L2 原始檔的結構契約 —— `S5PAdapter` 依賴的每一件事都在這裡釘住。

取代 `wip/check_o3_variables.py`。那支是印出來就算了的一次性診斷,而且本身有兩個問題:
檔名叫 `check_o3` 但實際檢查的是 SO₂,路徑寫死在**已不存在的舊佈局** `raw/SO2___/`
(現在是 `raw/L2/<product>/`)。它印出的東西正好就是 adapter 的輸入契約,所以改成斷言。

★ 這裡最值得釘住的是一個不對稱:**`latitude`/`longitude` 只有 NO₂ 是 xarray coords,
O₃/SO₂/HCHO 是一般變數**。adapter 用 `ds["latitude"]` 存取,兩種分類都拿得到 —— 但如果
哪天有人改成 `ds.coords["latitude"]`,就會只有 NO₂ 能跑。

需要真實 raw,碟沒掛就 skip。
"""
from __future__ import annotations

import glob

import numpy as np
import pytest

from src.config.catalog import PRODUCT_CONFIGS

from src.config.settings import DATA_ROOTS

PRODUCTS = ["NO2___", "O3____", "SO2___", "HCHO__"]
# 相對於 DATA_ROOTS 找。以前是 "/Volumes/*/..." 掃所有掛載碟,會連不相干的隨身碟
# 一起掃,而且在非 macOS 上根本沒有 /Volumes。
RAW_REL = "Sentinel-5P/raw/L2/{product}/*/*/*.nc"


def _sample(product):
    for root in DATA_ROOTS:
        fs = [f for f in sorted(glob.glob(str(root / RAW_REL.format(product=product))))
              if "/._" not in f]
        if fs:
            return fs[0]
    pytest.skip(f"{product}: 在 DATA_ROOTS 裡找不到 raw 樣本(外接碟未掛載?)")


@pytest.mark.requires_data
@pytest.mark.parametrize("product", PRODUCTS)
class TestS5PRawContract:
    def test_product_group_exists(self, product):
        import xarray as xr
        with xr.open_dataset(_sample(product), group="PRODUCT") as ds:
            assert len(ds.sizes) > 0

    def test_main_variable_matches_catalog(self, product):
        """catalog 記的 dataset_name 必須真的存在,且是 (time, scanline, ground_pixel)。"""
        import xarray as xr
        var = PRODUCT_CONFIGS[product].dataset_name
        with xr.open_dataset(_sample(product), group="PRODUCT") as ds:
            assert var in ds, f"{product}: catalog 寫 {var!r},檔案裡沒有"
            assert ds[var].dims == ("time", "scanline", "ground_pixel"), ds[var].dims
            assert ds[var].attrs.get("units") == "mol m-2"

    def test_qa_value_exists(self, product):
        """qa_value 是 regridder 的權重來源,缺了就沒有 QC。"""
        import xarray as xr
        with xr.open_dataset(_sample(product), group="PRODUCT") as ds:
            assert "qa_value" in ds

    def test_geolocation_accessible_regardless_of_coord_status(self, product):
        """lat/lon 必須用 ``ds[...]`` 拿得到 —— 不論它被歸類成 coord 還是 data_var。

        實測:NO₂ 的 lat/lon 是 coords,O₃/SO₂/HCHO 不是。adapter 走 ``ds["latitude"]``
        才對兩種都成立;若改用 ``ds.coords[...]`` 會只有 NO₂ 能跑。
        """
        import xarray as xr
        var = PRODUCT_CONFIGS[product].dataset_name
        with xr.open_dataset(_sample(product), group="PRODUCT") as ds:
            for name in ("latitude", "longitude"):
                assert name in ds.variables, f"{product}: 取不到 {name}"
                assert ds[name].shape == ds[var].shape, f"{product}: {name} 形狀不符"

    def test_adapter_reads_it(self, product):
        """契約的最終驗收:adapter 真的讀得出 GranuleL2。"""
        from src.processing.l3.adapters import S5PAdapter
        g = S5PAdapter(product).read(_sample(product))
        assert g is not None, f"{product}: adapter 回 None"
        assert g.values.ndim == 2 and g.lat.shape == g.values.shape
        assert g.qa is not None and np.isfinite(g.values).any()


@pytest.mark.requires_data
def test_hcho_has_usable_data_over_taiwan():
    """HCHO 曾被記為『qa>=0.5 台灣上空無資料 / 變數名待對』—— 兩者都已不成立。

    實測:變數名 `formaldehyde_tropospheric_vertical_column` 正確存在,台灣框內
    qa>=0.5 的有效點佔 97%,端到端聚合 25/25 天都有資料。留著這條免得又被誤記為壞掉。
    """
    from src.processing.l3.adapters import S5PAdapter
    g = S5PAdapter("HCHO__").read(_sample("HCHO__"))
    assert g is not None
    box = (g.lat >= 21) & (g.lat <= 26) & (g.lon >= 119) & (g.lon <= 123)
    if not box.any():
        pytest.skip("這一軌沒有掃到台灣")
    good = box & (g.qa >= 0.5) & np.isfinite(g.values)
    assert good.any(), "HCHO 在台灣框內 qa>=0.5 完全沒有有效點"
