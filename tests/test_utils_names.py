"""``extract_datetime_from_filename`` 與 ``nc_names.pick_name`` 的測試。

前者原本零測試,卻是 L3 聚合排序(``pipeline._sort_key``)與 GEMS 時間戳的隱含前提;
C12 要把 GEMS 三份 regex 收斂到它之前,先把行為釘住。全部合成,不需外接碟。
"""
from __future__ import annotations

from datetime import datetime

import numpy as np
import pytest
import xarray as xr

from src.utils.extract_datetime_from_filename import extract_datetime_from_filename as ext
from src.utils.nc_names import pick_name

CASES = [
    # S5P:兩個 T 時間,docstring 明寫要**開始時間**(第一個)
    ("S5P_OFFL_L2__NO2____20220101T042839_20220101T061009_21846_02_020301_20220101T203203.nc",
     datetime(2022, 1, 1, 4, 28, 39)),
    ("S5P_OFFL_L2__HCHO___20241231T033759_20241231T051929_37270_03_020601_20250101T195113.nc",
     datetime(2024, 12, 31, 3, 37, 59)),
    # MODIS:有 / 無時間、閏年第 366 天
    ("MOD04_L2.A2025001.0210.061.2025001180158.hdf", datetime(2025, 1, 1, 2, 10)),
    ("MCD19A2.A2025001.h29v06.061.2025003055754.hdf", datetime(2025, 1, 1)),
    ("MCD19A2.A2024366.h29v06.061.2025003055754.hdf", datetime(2024, 12, 31)),
    # GEMS:兩種真實命名(模式標籤可含連字號)
    ("GK2_GEMS_L2_20230515_0345_NO2_FW_DPRO_ORI.nc", datetime(2023, 5, 15, 3, 45)),
    ("GK2_GEMS_L2_20220101_0045_NO2_HE-ETC_DPRO_ORI.nc", datetime(2022, 1, 1, 0, 45)),
]


@pytest.mark.parametrize("name,expected", CASES, ids=[c[0][:22] for c in CASES])
def test_naive_utc(name, expected):
    assert ext(name, to_local=False) == expected


def test_local_conversion_is_plus_8():
    d = ext("GK2_GEMS_L2_20230515_0345_NO2_FW_DPRO_ORI.nc", to_local=True)
    assert d.tzinfo is not None and d.utcoffset().total_seconds() == 8 * 3600
    assert (d.hour, d.minute) == (11, 45)


def test_unparseable_returns_none():
    assert ext("random_file.nc", to_local=False) is None


def test_gems_invalid_date_raises():
    """記錄現況:GEMS 分支對無效日期會 raise;呼叫端(processor / adapter)要自己接成 None。"""
    with pytest.raises(ValueError):
        ext("GK2_GEMS_L2_20231399_2599_NO2_FW.nc", to_local=False)


class TestPickName:
    def test_first_match_across_variables_and_coords(self):
        ds = xr.Dataset({"no2": ("x", np.zeros(2))}, coords={"lat": ("x", np.zeros(2))})
        assert pick_name(ds, ("latitude", "lat")) == "lat"
        assert pick_name(ds, ("no2", "lat")) == "no2"
        assert pick_name(ds, ("foo", "bar")) is None


class TestGemsWrappers:
    """兩個 GEMS wrapper 委派給共用解析器後,「無效日期回 None」的既有契約不能丟。"""

    def test_processor_wrapper(self):
        from src.processing.gems_processor import gems_datetime_from_filename as g
        assert g("GK2_GEMS_L2_20230515_0345_NO2_FW_DPRO_ORI.nc") == datetime(2023, 5, 15, 3, 45)
        assert g("GK2_GEMS_L2_20231399_2599_NO2_FW.nc") is None

    def test_adapter_wrapper(self):
        from src.processing.l3.adapters.gems import _time_from_name as t
        assert t("GK2_GEMS_L2_20230515_0345_NO2_FW_DPRO_ORI.nc") == np.datetime64("2023-05-15T03:45", "ns")
        assert t("GK2_GEMS_L2_20231399_2599_NO2_FW.nc") is None
