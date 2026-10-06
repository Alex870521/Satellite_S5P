"""逐軌輸出(freq='granule')與過境時刻。

S5P 檔案的 ``time`` 只是當天 00:00 的參考時間;逐軌輸出要的是掃過目標網格那一兩分鐘的
時刻,而且同日兩軌不能被合併。這裡用合成 swath 驗證,不需要真實資料。
"""
from __future__ import annotations

import numpy as np
import pytest
import xarray as xr

from src.config.catalog import PRODUCT_CONFIGS
from src.processing.l3 import GridSpec, GranuleL2, L3Pipeline, SupersampleBinRegridder
from src.processing.l3.adapters.modis import _date_from_name
from src.processing.l3.adapters.s5p import _scan_times
from src.processing.l3.pipeline import _overpass_time, _period_key

BOUNDS = (119.0, 123.0, 21.0, 26.0)
DAY = np.datetime64("2024-01-01", "ns")


def _swath(t0: np.datetime64, n=60, m=30, lat0=15.0, lat1=30.0, value=1.0) -> GranuleL2:
    """沿軌道從 lat0 掃到 lat1,每條掃描線比前一條晚 1 秒。台灣框只佔其中一段。"""
    lat, lon = np.meshgrid(np.linspace(lat0, lat1, n), np.linspace(119.5, 122.5, m),
                           indexing="ij")
    scan = t0 + np.arange(n).astype("timedelta64[s]").astype("timedelta64[ns]")
    return GranuleL2(values=np.full((n, m), value), lon=lon, lat=lat, time=DAY,
                     product=PRODUCT_CONFIGS["NO2___"], qa=np.ones((n, m)),
                     source="TEST", file_name="synth.nc", scan_time=scan)


class _FakeAdapter:
    """檔名 → 預先做好的 granule,讓 aggregate 不必讀真檔。"""
    product = PRODUCT_CONFIGS["NO2___"]
    source = "TEST"

    def __init__(self, granules: dict[str, GranuleL2]):
        self.granules = granules

    def read(self, path):
        return self.granules[path.name]


def _pipe(granules):
    grid = GridSpec.from_degrees(0.1, BOUNDS)
    return L3Pipeline(_FakeAdapter(granules), SupersampleBinRegridder(K=2), grid)


class TestOverpassTime:
    def test_mean_of_scanlines_inside_grid(self):
        g = _swath(np.datetime64("2024-01-01T04:00:00", "ns"))
        grid = GridSpec.from_degrees(0.1, BOUNDS)
        inside = ((g.lat >= grid.lat.min()) & (g.lat <= grid.lat.max())).any(axis=1)
        expect = g.scan_time[inside].astype("int64").mean()
        got = _overpass_time(g, grid)
        assert got.astype("int64") == pytest.approx(expect, abs=1)
        # 不是整圈的起點,也不是午夜參考時間
        assert got != DAY and got > g.scan_time[0]

    def test_without_scan_time_keeps_time(self):
        g = _swath(np.datetime64("2024-01-01T04:00:00", "ns"))
        g.scan_time = None
        assert _overpass_time(g, GridSpec.from_degrees(0.1, BOUNDS)) == DAY

    def test_period_key_granule_is_exact_time(self):
        t = np.datetime64("2024-03-15T04:30:12.5", "ns")
        assert _period_key(t, "granule") == t
        assert _period_key(t, "granule", tz_offset_hours=8) == t   # 逐軌不套時區


class TestGranuleAggregate:
    FILES = {"S5P_OFFL_L2__NO2____20240101T033000_20240101T051000_1.nc":
             _swath(np.datetime64("2024-01-01T03:30:00", "ns"), value=1.0),
             "S5P_OFFL_L2__NO2____20240101T051000_20240101T065000_2.nc":
             _swath(np.datetime64("2024-01-01T05:10:00", "ns"), value=3.0)}

    def test_two_orbits_same_day_stay_separate(self, tmp_path):
        pipe = _pipe(self.FILES)
        files = [tmp_path / n for n in self.FILES]
        out = pipe.aggregate(files, freq="granule", level="L2")
        assert len(out) == 2
        (t1, r1), (t2, r2) = out
        assert t1 < t2 and t1.astype("datetime64[D]") == t2.astype("datetime64[D]")
        assert np.nanmean(r1["value"]) == pytest.approx(1.0)
        assert np.nanmean(r2["value"]) == pytest.approx(3.0)

    def test_daily_still_merges_them(self, tmp_path):
        files = [tmp_path / n for n in self.FILES]
        out = _pipe(self.FILES).aggregate(files, freq="D", level="L2")
        assert len(out) == 1 and out[0][0] == np.datetime64("2024-01-01")

    def test_same_time_raises(self, tmp_path):
        g = _swath(np.datetime64("2024-01-01T04:00:00", "ns"))
        names = ["MCD19A2.A2024001.h28v06.061.1.nc", "MCD19A2.A2024001.h29v06.061.2.nc"]
        dup = {n: GranuleL2(values=g.values, lon=g.lon, lat=g.lat, time=DAY,
                            product=g.product, qa=g.qa, file_name=n) for n in names}
        with pytest.raises(ValueError, match="逐軌"):
            _pipe(dup).aggregate([tmp_path / n for n in names], freq="granule", level="L2")


class TestFileTimes:
    def test_modis_granule_name_has_hhmm(self):
        assert _date_from_name("MYD04_L2.A2023032.0505.061.2023033.hdf") == \
            np.datetime64("2023-02-01T05:05", "ns")

    def test_modis_daily_tile_is_date_only(self):
        assert _date_from_name("MCD19A2.A2023032.h28v06.061.2023034.hdf") == \
            np.datetime64("2023-02-01", "ns")

    def test_s5p_undecoded_delta_time_in_ms(self):
        ds = xr.Dataset({"delta_time": (("time", "scanline"), np.array([[0.0, 1500.0, 60000.0]]))})
        got = _scan_times(ds, DAY)
        assert list(got) == [DAY, DAY + np.timedelta64(1500, "ms"), DAY + np.timedelta64(60, "s")]
