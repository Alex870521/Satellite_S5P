"""C11:把靜默失敗變成有訊息的失敗 —— 行為不變,只多訊息。

三個能單測的點:
* ``pipeline._name_sort_key`` 檔名日期解析失敗時退回字典序 → 必須發 warning,
  否則 ``aggregate`` 的重複期別保護會在毫無線索的情況下 raise。
* ``SupersampleBinRegridder`` 角點推導失敗回全 NaN 場 → 必須 log,
  否則統計上與「當天無觀測」無法區分。
* ``SatelliteHub._setup_timezone`` 無效時區退回 UTC → 必須 warning。
"""
from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pytest

from src.processing.l3 import GranuleL2, GridSpec, SupersampleBinRegridder
from src.config.catalog import PRODUCT_CONFIGS

BOUNDS = (119.0, 123.0, 21.0, 26.0)


class TestSortKeyDegradation:
    def test_unparseable_name_falls_back_and_warns(self, caplog):
        from src.processing.l3.pipeline import _name_sort_key
        with caplog.at_level(logging.WARNING, logger="src.processing.l3.pipeline"):
            key = _name_sort_key(Path("no_date_here.nc"))
        assert key == (1, "", "no_date_here.nc")           # 行為:退回字典序桶
        assert any("no_date_here.nc" in r.message for r in caplog.records)

    def test_parseable_name_is_silent(self, caplog):
        from src.processing.l3.pipeline import _name_sort_key
        with caplog.at_level(logging.WARNING, logger="src.processing.l3.pipeline"):
            key = _name_sort_key(Path("GK2_GEMS_L2_20230515_0345_NO2_FW_DPRO_ORI.nc"))
        assert key[0] == 0 and not caplog.records


class TestRegridderCornerFailure:
    def test_too_small_granule_gives_nan_field_and_logs(self, caplog):
        # (2, 1) 推不出角點:GEMS 裁切曾真的切出 (211, 1) 這種條帶
        lat = np.array([[22.0], [22.1]]); lon = np.array([[120.0], [120.0]])
        g = GranuleL2(values=np.ones((2, 1)), lon=lon, lat=lat,
                      time=np.datetime64("2024-01-01", "ns"), product=PRODUCT_CONFIGS["NO2___"],
                      qa=np.ones((2, 1)), source="TEST", file_name="tiny_strip.nc")
        grid = GridSpec.from_degrees(0.05, BOUNDS)
        with caplog.at_level(logging.DEBUG, logger="src.processing.l3.regridder"):
            gf = SupersampleBinRegridder(K=4).regrid(g, grid)
        assert np.isnan(gf.value).all() and gf.count.sum() == 0   # 行為:空場
        assert any("tiny_strip.nc" in r.message for r in caplog.records)


class TestTimezoneFallback:
    def test_invalid_timezone_falls_back_to_utc_and_warns(self, caplog):
        from src.api.core import SatelliteHub

        class _Hub(SatelliteHub):                      # 抽象方法補空實作
            def authentication(self, *a, **k): ...
            def fetch_data(self, *a, **k): ...
            def download_data(self, *a, **k): ...
            def process_data(self, *a, **k): ...

        h = _Hub.__new__(_Hub)                          # 不跑 __init__(它會建目錄)
        with caplog.at_level(logging.WARNING, logger="src.api.core"):
            h._setup_timezone("Not/AZone")
        assert str(h.timezone) == "UTC"
        assert any("Not/AZone" in r.message for r in caplog.records)
