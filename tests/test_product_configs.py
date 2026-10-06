"""Phase 2(C3 / C10):產品設定的單一真相。

D1/D2 拍板(2026-09-20):**變數都保留給使用者** —— AER_AI 兩個波段對都可選、
GEMS 六項全部進 catalog。這裡先把「改之前」的值釘成 golden,收斂後逐欄不得變。
"""
from __future__ import annotations

import pytest

from src.config.catalog import PRODUCT_CONFIGS

# 2026-09-20 自 gems_processor.PRODUCTS 抄下的原值(title/units 的字串格式兩邊本就不同,不在此比)
GEMS_PROCESSOR_ORIGINAL = {
    "NO2":    dict(display_name="NO₂",  dataset_name="ColumnAmountNO2",          vmin=0,   vmax=1.0e16, cmap="turbo"),
    "O3T":    dict(display_name="O₃",   dataset_name="ColumnAmountO3",           vmin=200, vmax=400,    cmap="viridis"),
    "HCHO":   dict(display_name="HCHO", dataset_name="ColumnAmountHCHO",         vmin=0,   vmax=2.0e16, cmap="turbo"),
    "SO2":    dict(display_name="SO₂",  dataset_name="ColumnAmountSO2",          vmin=0,   vmax=1.0e16, cmap="turbo"),
    "AERAOD": dict(display_name="AOD",  dataset_name="FinalAerosolOpticalDepth", vmin=0,   vmax=2.0,    cmap="YlOrBr"),
    "UVI":    dict(display_name="UVI",  dataset_name="UVIndex",                  vmin=0,   vmax=12,     cmap="magma"),
}
AER_AI_BANDS = {"aerosol_index_340_380", "aerosol_index_354_388"}


class TestGemsConfigMatchesCatalog:
    @pytest.mark.parametrize("friendly", list(GEMS_PROCESSOR_ORIGINAL))
    def test_processor_products_keep_original_values(self, friendly):
        """現況即通過;收斂到 catalog 之後**仍須**通過 —— 保證沒動任何科學設定。"""
        from src.processing.gems_processor import GEMSProcessor
        cfg = GEMSProcessor.PRODUCTS[friendly]
        for k, v in GEMS_PROCESSOR_ORIGINAL[friendly].items():
            assert getattr(cfg, k) == v, (friendly, k)

    @pytest.mark.parametrize("friendly", list(GEMS_PROCESSOR_ORIGINAL))
    def test_catalog_has_every_gems_product_with_same_values(self, friendly):
        cfg = PRODUCT_CONFIGS[f"GEMS_{friendly}"]
        for k, v in GEMS_PROCESSOR_ORIGINAL[friendly].items():
            assert getattr(cfg, k) == v, (friendly, k)

    def test_processor_reads_from_catalog_not_its_own_copy(self):
        from src.processing.gems_processor import GEMSProcessor
        for f in GEMS_PROCESSOR_ORIGINAL:
            assert GEMSProcessor.PRODUCTS[f] is PRODUCT_CONFIGS[f"GEMS_{f}"], f

    def test_runner_knows_every_gems_raw_dir(self):
        from src.processing.l3.runner import GEMS_RAW_DIR, SHORT_NAME
        for f in GEMS_PROCESSOR_ORIGINAL:
            assert f"GEMS_{f}" in GEMS_RAW_DIR and f"GEMS_{f}" in SHORT_NAME, f


class TestAerAiBands:
    def test_catalog_and_registry_agree_on_both_bands(self):
        """B2 的根治:兩處各自做了不同科學選擇後失去同步 → 改成兩處都列出同一組候選。"""
        from src.coverage.registry import _S5P_VARS
        cat = PRODUCT_CONFIGS["AER_AI"]
        cat_set = {cat.dataset_name, *getattr(cat, "alt_dataset_names", ())}
        reg = _S5P_VARS["AER_AI"]
        reg_set = set(reg) if isinstance(reg, (tuple, list)) else {reg}
        assert cat_set == AER_AI_BANDS, cat_set
        assert reg_set == AER_AI_BANDS, reg_set

    def test_default_band_is_unchanged_for_existing_files(self):
        """既有已處理檔寫的是 340_380;預設不能變,否則舊檔立刻讀不到。"""
        assert PRODUCT_CONFIGS["AER_AI"].dataset_name == "aerosol_index_340_380"


class TestAerAiUserChoice:
    """使用者用 variable= 選波段;預設不變、非法值要擋、reader 兩個都讀得到。"""

    def _proc(self, **kw):
        from src.processing.sentinel_processor import SentinelProcessor
        return SentinelProcessor(file_type="AER_AI", resolution=(5.5, 3.5), **kw)

    def test_default_is_340_380(self):
        assert self._proc().dataset_name == "aerosol_index_340_380"

    def test_user_can_pick_354_388(self):
        assert self._proc(variable="aerosol_index_354_388").dataset_name == "aerosol_index_354_388"

    def test_unknown_variable_is_rejected(self):
        with pytest.raises(ValueError):
            _ = self._proc(variable="aerosol_index_999").dataset_name

    @pytest.mark.parametrize("band", sorted(AER_AI_BANDS))
    def test_reader_resolves_whichever_band_the_file_has(self, band):
        import types
        import numpy as np
        import xarray as xr
        import src.coverage.reader as R
        from src.coverage.registry import get_spec
        cls = next(v for v in vars(R).values() if isinstance(v, type) and hasattr(v, "_resolve_var"))
        ds = xr.Dataset({band: ("x", np.zeros(3)), "qa_value": ("x", np.ones(3))})
        fake = types.SimpleNamespace(spec=get_spec("sentinel5p"))
        assert cls._resolve_var(fake, ds, "AER_AI") == band
