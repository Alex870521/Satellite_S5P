"""統一 L3 regrid pipeline 的測試。

分兩層:

* **純邏輯**(不需外接碟)—— 網格精確性、超取樣的守恆性質、時間聚合、比對指標。
  用合成資料,CI 一定跑得動。
* **需要真實資料**(``requires_data`` 標記)—— 三個 source 的 adapter 讀真檔、
  以及跟 HARP oracle 對答案。碟沒掛/HARP 沒裝就 skip,不算失敗。

取代原本散在 gitignore 的 `wip_l3/` 裡、且各自複製了一份 production 邏輯的驗證腳本。
"""
from __future__ import annotations

import glob
from pathlib import Path

import numpy as np
import pytest

from src.processing.l3 import (GridSpec, GranuleL2, L3Accumulator, L3Pipeline,
                               L3Writer, SupersampleBinRegridder,
                               corners_from_centers, detect_level)
from src.processing.l3.compare import compare_fields
from src.config.catalog import PRODUCT_CONFIGS

BOUNDS = (119.0, 123.0, 21.0, 26.0)

# 真實資料樣本(跨兩顆碟);不存在就 skip
SAMPLES = {
    "s5p": ("NO2___", "/Volumes/Transcend/Sentinel-5P/raw/L2/NO2___/2024/*/*.nc"),
    "modis": ("MCD19A2", "/Volumes/Transcend/MODIS/raw/MCD19A2/2023/01/*.hdf"),
    "gems": ("GEMS_NO2", "/Volumes/TOSHIBA/GEMS/raw/NO2/*/*/*.nc"),
}


def _first_sample(key):
    _, pattern = SAMPLES[key]
    fs = [f for f in sorted(glob.glob(pattern)) if "/._" not in f]
    return fs[0] if fs else None


def _synth_granule(product="NO2___", n=40, m=30, value=1.0):
    """合成一個落在台灣框內的規則 swath。"""
    lat, lon = np.meshgrid(np.linspace(21.5, 25.5, n), np.linspace(119.5, 122.5, m),
                           indexing="ij")
    return GranuleL2(values=np.full((n, m), value, dtype="float64"),
                     lon=lon, lat=lat, time=np.datetime64("2024-01-01", "ns"),
                     product=PRODUCT_CONFIGS[product],
                     qa=np.ones((n, m)), source="TEST", file_name="synth.nc")


# ----------------------------------------------------------------- 網格
class TestGridSpec:
    def test_from_degrees_shape(self):
        g = GridSpec.from_degrees(0.02, BOUNDS)
        assert (len(g.lat), len(g.lon)) == (251, 201)

    def test_from_degrees_matches_model_grid(self):
        """統一解析度的核心保證:必須逐格等於模型的 TARGET_LAT/LON。"""
        g = GridSpec.from_degrees(0.02, BOUNDS)
        assert np.abs(g.lat - np.round(np.arange(21.0, 26.0 + 1e-6, 0.02), 6)).max() == 0
        assert np.abs(g.lon - np.round(np.arange(119.0, 123.0 + 1e-6, 0.02), 6)).max() == 0

    def test_from_degrees_uniform_step(self):
        g = GridSpec.from_degrees(0.02, BOUNDS)
        assert np.unique(np.round(np.diff(g.lat), 9)).tolist() == [0.02]

    def test_edges_are_cells_plus_one(self):
        g = GridSpec.from_degrees(0.02, BOUNDS)
        assert len(g.lat_edges) == len(g.lat) + 1
        assert len(g.lon_edges) == len(g.lon) + 1

    def test_km_mode_still_works(self):
        """回歸:加了度數模式不能破壞原本的 km 模式。"""
        k = GridSpec(resolution=(5.5, 3.5))
        assert len(k.lat) > 0 and len(k.lon) > 0

    def test_deg_requires_bounds(self):
        with pytest.raises(ValueError):
            GridSpec(deg=(0.02, 0.02))

    def test_needs_one_of_resolution_or_deg(self):
        with pytest.raises(ValueError):
            GridSpec()


# ----------------------------------------------------------------- regrid
class TestSupersample:
    def test_constant_field_is_preserved(self):
        """常數場經超取樣後,有觀測的格必須仍是同一個常數(加權平均的正確性)。"""
        g = _synth_granule(value=7.0)
        grid = GridSpec.from_degrees(0.02, BOUNDS)
        gf = SupersampleBinRegridder(K=4).regrid(g, grid)
        finite = np.isfinite(gf.value)
        assert finite.any()
        assert np.allclose(gf.value[finite], 7.0)

    def test_output_shape_matches_grid(self):
        grid = GridSpec.from_degrees(0.02, BOUNDS)
        gf = SupersampleBinRegridder(K=4).regrid(_synth_granule(), grid)
        assert gf.value.shape == (len(grid.lat), len(grid.lon))

    def test_qa_threshold_filters(self):
        """qa 全部低於門檻 → 整張應為 NaN。"""
        g = _synth_granule()
        g.qa = np.zeros_like(g.qa)
        grid = GridSpec.from_degrees(0.02, BOUNDS)
        gf = SupersampleBinRegridder(K=4, qa_threshold=0.5).regrid(g, grid)
        assert not np.isfinite(gf.value).any()

    def test_corners_from_centers_shape(self):
        a = np.random.rand(10, 8)
        assert corners_from_centers(a).shape == (11, 9)

    @pytest.mark.parametrize("shape", [(1, 50), (2, 2), (50, 1)])
    def test_corners_rejects_degenerate_granule(self, shape):
        """太小的 granule 推不出角點,必須 raise 而不是 IndexError。

        真的會遇到:GEMS 區域裁切曾切出 (211, 1) 的一像元寬條帶,舊版在邊界外插
        `p[:, 0] = 2*p[:, 1] - p[:, 2]` 直接 IndexError,把整批 847 天的聚合帶掉。
        """
        with pytest.raises(ValueError):
            corners_from_centers(np.zeros(shape))

    def test_regrid_survives_degenerate_granule(self):
        """一軌太小不該讓整批處理崩掉 —— 回空場繼續跑。"""
        g = _synth_granule()
        g.values = g.values[:, :1]
        g.lat = g.lat[:, :1]
        g.lon = g.lon[:, :1]
        g.qa = g.qa[:, :1]
        grid = GridSpec.from_degrees(0.05, BOUNDS)
        gf = SupersampleBinRegridder(K=4).regrid(g, grid)
        assert gf.value.shape == (len(grid.lat), len(grid.lon))
        assert not np.isfinite(gf.value).any()

    def test_result_is_independent_of_grid_extent(self):
        """同一格的值不可以因為網格範圍不同而改變。

        篩選框若只用像元**中心**過濾,中心在網格外、footprint 卻蓋進來的像元會被丟掉,
        最外圈就少了貢獻者 —— 實測 S5P 單軌時邊界 1~2 圈有 0.33% 的格偏掉(部分 >10%)。
        把網格外擴再裁回,重疊區必須逐格相同。
        """
        n, m = 60, 50
        lat, lon = np.meshgrid(np.linspace(20.5, 26.5, n), np.linspace(118.5, 123.5, m),
                               indexing="ij")
        g = GranuleL2(values=np.linspace(1, 2, n * m).reshape(n, m),
                      lon=lon, lat=lat, time=np.datetime64("2024-01-01", "ns"),
                      product=PRODUCT_CONFIGS["NO2___"], qa=np.ones((n, m)))
        rg = SupersampleBinRegridder(K=4)
        tight = GridSpec.from_degrees(0.05, (119.0, 123.0, 21.0, 26.0))
        wide = GridSpec.from_degrees(0.05, (118.8, 123.2, 20.8, 26.2))
        a = rg.regrid(g, tight).value
        b = rg.regrid(g, wide).value
        la = np.isclose(wide.lat[:, None], tight.lat[None, :]).any(axis=1)
        lo = np.isclose(wide.lon[:, None], tight.lon[None, :]).any(axis=1)
        bc = b[np.ix_(la, lo)]
        both = np.isfinite(a) & np.isfinite(bc)
        assert both.any()
        assert np.abs(a[both] - bc[both]).max() == 0
        # 覆蓋也不該因為網格範圍而少
        assert np.isfinite(a).sum() == np.isfinite(bc).sum()


# ----------------------------------------------------------------- 聚合
class TestAggregation:
    def test_accumulator_matches_plain_mean(self):
        """等權重下,accumulator 的加權平均必須等於單純平均。"""
        grid = GridSpec.from_degrees(0.1, BOUNDS)
        rg = SupersampleBinRegridder(K=2)
        fields = [rg.regrid(_synth_granule(value=v), grid) for v in (1.0, 3.0)]
        acc = L3Accumulator(grid)
        for f in fields:
            acc.add(f)
        out = acc.finalize()["value"]
        both = np.isfinite(fields[0].value) & np.isfinite(fields[1].value)
        assert np.allclose(out[both], 2.0)

    def test_period_key_daily_monthly(self):
        from src.processing.l3.pipeline import _period_key
        t = np.datetime64("2024-03-15T04:30", "ns")
        assert _period_key(t, "D") == np.datetime64("2024-03-15")
        assert _period_key(t, "M") == np.datetime64("2024-03")
        assert _period_key(t, "Y") == np.datetime64("2024")

    def test_period_key_rejects_bad_freq(self):
        from src.processing.l3.pipeline import _period_key
        with pytest.raises(ValueError):
            _period_key(np.datetime64("2024-01-01", "ns"), "W")


# ----------------------------------------------------------------- L2/L3 分流
class TestLevelRoundTrip:
    """把 regrid 結果寫成 L3 nc 後,必須被認成 L3 且能無損 ingest 回來。

    這是 L2/L3 自動分流最強的自驗:regrid → 寫檔 → 偵測 → 讀回,逐格必須一致。
    (原本在 wip_l3/regression_l3_ingest.py,需要真實 granule;改用合成資料後
    不需外接碟也能跑。)
    """

    def _write_l3(self, tmp_path):
        grid = GridSpec.from_degrees(0.05, BOUNDS)
        gf = SupersampleBinRegridder(K=4).regrid(_synth_granule(value=2.5), grid)
        out = Path(tmp_path) / "l3.nc"
        L3Writer().write_nc(gf, out)
        return grid, gf, out

    def test_written_l3_is_detected_as_l3(self, tmp_path):
        _, _, out = self._write_l3(tmp_path)
        assert detect_level(out) == "L3"

    def test_ingest_roundtrip_is_lossless(self, tmp_path):
        """同一張網格 → passthrough,值必須逐格不變。"""
        from src.processing.l3 import ingest_l3
        grid, gf, out = self._write_l3(tmp_path)
        back = ingest_l3(out, grid, gf.product)
        both = np.isfinite(gf.value) & np.isfinite(back.value)
        assert both.any()
        assert np.abs(gf.value[both] - back.value[both]).max() == 0

    def test_pipeline_dispatches_l3_automatically(self, tmp_path):
        """build_field 對 L3 檔要自動走 ingest,不再 regrid。"""
        from src.processing.l3.adapters import S5PAdapter
        grid, gf, out = self._write_l3(tmp_path)
        pipe = L3Pipeline(S5PAdapter("NO2___"), SupersampleBinRegridder(K=4), grid, L3Writer())
        back = pipe.build_field(out)
        assert back is not None
        both = np.isfinite(gf.value) & np.isfinite(back.value)
        assert np.abs(gf.value[both] - back.value[both]).max() == 0

    def test_ingest_onto_different_grid_aligns(self, tmp_path):
        """不同網格 → 走線性內插對齊,形狀必須跟著目標網格。"""
        from src.processing.l3 import ingest_l3
        _, gf, out = self._write_l3(tmp_path)
        coarse = GridSpec.from_degrees(0.1, BOUNDS)
        back = ingest_l3(out, coarse, gf.product)
        assert back.value.shape == (len(coarse.lat), len(coarse.lon))


# ----------------------------------------------------------------- 比對指標
class TestCompare:
    def test_identical_fields(self):
        a = np.random.rand(5, 4, 3)
        m = compare_fields(a, a)
        assert m["cell_r"] == pytest.approx(1.0)
        assert m["cell_bias_pct"] == pytest.approx(0.0, abs=1e-9)

    def test_bias_is_measured_on_common_mask_only(self):
        """A 獨有的格不可以影響偏差 —— 這正是 SO2 +19.77% 假象的成因。"""
        a = np.ones((2, 3, 3))
        b = np.ones((2, 3, 3))
        a[0, 0, 0], b[0, 0, 0] = -999.0, np.nan      # A 有、B 無:應被排除
        m = compare_fields(a, b)
        assert m["cell_bias_pct"] == pytest.approx(0.0, abs=1e-9)
        assert m["only_a_mean"] == pytest.approx(-999.0)

    def test_shape_mismatch_raises(self):
        with pytest.raises(ValueError):
            compare_fields(np.ones((2, 2)), np.ones((3, 3)))


# ----------------------------------------------------------------- 真實資料
@pytest.mark.requires_data
class TestRealData:
    @pytest.mark.parametrize("key", list(SAMPLES))
    def test_adapter_reads_to_common_grid(self, key):
        from src.processing.l3.runner import make_adapter
        f = _first_sample(key)
        if f is None:
            pytest.skip(f"{key}: 找不到樣本檔(外接碟未掛載?)")
        g = make_adapter(key, SAMPLES[key][0]).read(f)
        if g is None:
            pytest.skip(f"{key}: 該檔無有效資料")
        grid = GridSpec.from_degrees(0.02, BOUNDS)
        gf = SupersampleBinRegridder(K=4).regrid(g, grid)
        assert gf.value.shape == (251, 201)

    def test_detect_level_on_real_files(self):
        for key in ("s5p", "modis"):
            f = _first_sample(key)
            if f is None:
                continue
            assert detect_level(f) == "L2"

    # 多氣體 × 多軌:沿用 wip_l3/validate_supersample.py 的覆蓋面
    # (實測 r min 0.996 / mean 0.999;HCHO 尚未驗過,見 l3/README)
    HARP_CASES = [
        ("NO2___", "/Volumes/Transcend/Sentinel-5P/raw/L2/NO2___/{ym}/*.nc"),
        ("O3____", "/Volumes/Transcend/Sentinel-5P/raw/L2/O3____/{ym}/*.nc"),
    ]

    @pytest.mark.parametrize("product,pattern", HARP_CASES)
    @pytest.mark.parametrize("ym", ["2023/03", "2023/07", "2023/12"])
    def test_vs_harp_oracle(self, product, pattern, ym):
        """自建超取樣 vs HARP bin_spatial:逐格 r 必須 >= 0.99。"""
        from src.processing.l3.harp_oracle import harp_available, harp_oracle
        from src.processing.l3.runner import make_adapter
        if not harp_available():
            pytest.skip("HARP CLI 未安裝(micromamba create -n harp -c conda-forge harp)")
        fs = [f for f in sorted(glob.glob(pattern.format(ym=ym))) if "/._" not in f]
        if not fs:
            pytest.skip(f"找不到 {product} {ym} 的樣本檔")
        grid = GridSpec(resolution=(5.5, 3.5))          # 用 km 網格對齊 HARP 驗證慣例
        oracle = harp_oracle(fs[0], product, grid)
        if oracle is None:
            pytest.skip("HARP 未產出 oracle(該軌可能無有效資料)")
        g = make_adapter("s5p", product).read(fs[0])
        if g is None:
            pytest.skip("adapter 讀不到有效資料")
        gf = SupersampleBinRegridder(K=4).regrid(g, grid)
        m = compare_fields(oracle, gf.value)
        assert m["n_common"] > 50
        assert m["cell_r"] >= 0.99, f"{product} {ym}: r={m['cell_r']:.4f}"
