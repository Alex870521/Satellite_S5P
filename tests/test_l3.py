"""統一 L3 regrid pipeline 的測試。

分兩層:

* **純邏輯**(不需外接碟)—— 網格精確性、超取樣的守恆性質、時間聚合、比對指標。
  用合成資料,CI 一定跑得動。
* **需要真實資料**(``requires_data`` 標記)—— 三個 source 的 adapter 讀真檔、
  以及跟 HARP oracle 對答案。碟沒掛/HARP 沒裝就 skip,不算失敗。

"""
from __future__ import annotations

import glob
from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from src.processing.l3 import (GridSpec, GranuleL2, L3Accumulator, L3Pipeline,
                               L3Writer, SupersampleBinRegridder,
                               corners_from_centers, detect_level)
from src.processing.l3.compare import compare_fields
from src.config.catalog import PRODUCT_CONFIGS

BOUNDS = (119.0, 123.0, 21.0, 26.0)

# 真實資料樣本:相對於 DATA_ROOTS 的任何一個根目錄去找,找不到就 skip。
# (資料可能散在多顆碟,而且碟名因人而異 → 不要寫死掛載點,用 SATELLITE_DATA_ROOTS 指定。)
from src.config.settings import DATA_ROOTS

SAMPLES = {
    "s5p": ("NO2___", "Sentinel-5P/raw/L2/NO2___/2024/*/*.nc"),
    "modis": ("MCD19A2", "MODIS/raw/MCD19A2/2023/01/*.hdf"),
    "gems": ("GEMS_NO2", "GEMS/raw/NO2/*/*/*.nc"),
}


def _glob_roots(rel_pattern):
    """在每個資料根目錄底下找;回傳第一個有命中的完整 glob 結果。"""
    for root in DATA_ROOTS:
        hits = [f for f in sorted(glob.glob(str(root / rel_pattern))) if "/._" not in f]
        if hits:
            return hits
    return []


def _first_sample(key):
    fs = _glob_roots(SAMPLES[key][1])
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
def _write_synth_gems(tmp_path):
    """合成一個 **GEMS 版面**的 L2 nc:root 空、座標在 ``Geolocation Fields`` group、
    資料在 ``Data Fields`` group。detect_level 只看結構,所以這樣就夠。"""
    lat, lon = np.meshgrid(np.linspace(21.5, 25.5, 6), np.linspace(119.5, 122.5, 5),
                           indexing="ij")
    out = Path(tmp_path) / "GK2_GEMS_L2_synth_NO2.nc"
    xr.Dataset().to_netcdf(out)
    xr.Dataset({"Latitude": (("spatial", "image"), lat),
                "Longitude": (("spatial", "image"), lon)}
               ).to_netcdf(out, group="Geolocation Fields", mode="a")
    xr.Dataset({"ColumnAmountNO2Trop": (("spatial", "image"), np.ones_like(lat))}
               ).to_netcdf(out, group="Data Fields", mode="a")
    return out


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
    用合成資料,不需外接碟也能跑。
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
class TestDetectLevelGroupedFiles:
    """座標不在 root 的 L2 檔(GEMS 的 ``Geolocation Fields``)也要認得出來。

    2026-09-09 之前 detect_level 沒有這條分支:GEMS 走 L3Pipeline 會在這裡 raise,
    而 collocate 那條路不經 build_field,所以上千天都沒踩到。用合成檔,不需外接碟。
    """

    def test_gems_layout_is_l2(self, tmp_path):
        assert detect_level(_write_synth_gems(tmp_path)) == "L2"

    def test_empty_root_without_known_group_still_raises(self, tmp_path):
        out = Path(tmp_path) / "empty.nc"
        xr.Dataset().to_netcdf(out)
        with pytest.raises(ValueError):
            detect_level(out)


class TestGemsWiring:
    """GEMS 的 adapter key 要同時在 catalog、SHORT_NAME、GEMS_RAW_DIR 三處對得上,
    CLI 也不能再把目錄名當 key 吃進去(那是先前 KeyError 的來源)。"""

    def test_every_gems_key_is_wired_everywhere(self):
        from src.processing.l3.runner import GEMS_RAW_DIR, SHORT_NAME
        for k in GEMS_RAW_DIR:
            assert k in SHORT_NAME, k
            assert k in PRODUCT_CONFIGS, k
        shorts = [SHORT_NAME[k] for k in GEMS_RAW_DIR]
        assert len(set(shorts)) == len(shorts), "同一個 NO2 檔的三個柱量短名不可相撞"

    def test_cli_rejects_directory_name_for_gems(self, tmp_path):
        from scripts.l3_regrid_year import _discover
        with pytest.raises(SystemExit):
            _discover("gems", "NO2", 2022, [Path(tmp_path)])
        assert _discover("gems", "GEMS_NO2_TROP", 2022, [Path(tmp_path)]) == []


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
        for key in ("s5p", "modis", "gems"):
            f = _first_sample(key)
            if f is None:
                continue
            assert detect_level(f) == "L2"

    def test_gems_through_pipeline_build_field(self):
        """GEMS 走 L3Pipeline(不傳 level → 必經 detect_level)不得 raise。
        這正是先前斷掉、且沒有任何測試蓋到的那條路。"""
        from src.processing.l3.runner import make_adapter
        f = _first_sample("gems")
        if f is None:
            pytest.skip("gems: 找不到樣本檔(外接碟未掛載?)")
        pipe = L3Pipeline(make_adapter("gems", "GEMS_NO2_TROP"),
                          SupersampleBinRegridder(K=4, qa_threshold=0.5),
                          GridSpec.from_degrees(0.02, BOUNDS), L3Writer())
        gf = pipe.build_field(f)
        assert gf is None or gf.value.shape == (251, 201)

    # 六種 S5P 產品 × 三個月,對 HARP bin_spatial 逐格比對(2026-10-06 擴充;原本只有 NO2/O3)。
    # 年份挑工作碟上有原始檔的那年;產品碼 → (年份, 網格, r 門檻)。
    #   * 門檻 0.99:NO2 / O3 / CO / CH4 實測 0.991–0.9997。
    #   * 門檻 0.97:SO2 / HCHO 實測 0.975–0.991。訊號弱、像元間雜訊大,子點取樣與 HARP
    #     精確多邊形面積的差異被放大;K 從 4 → 8 → 16 時 r 隨之上升(HCHO 0.984→0.990→0.991),
    #     確認是 K=4 的取樣精細度而非方法錯誤。K=4 是刻意的研究預設,不為測試改它。
    #   * CH4 用東亞陸地框:短波紅外線反演在海面與雲下無值,台灣框實測兩邊都是 0 格。
    #   偏差全部在 ±1% 內。
    _TW = dict(resolution=(5.5, 3.5))
    _EA_CH4 = dict(resolution=(5.5, 7.0), bounds=(100, 135, 15, 45))
    HARP_CASES = [
        ("NO2___", "2024", _TW, 0.99),
        ("O3____", "2022", _TW, 0.99),
        ("CO____", "2023", _TW, 0.99),
        ("CH4___", "2023", _EA_CH4, 0.99),
        ("SO2___", "2024", _TW, 0.97),
        ("HCHO__", "2023", _TW, 0.97),
    ]

    @pytest.mark.parametrize("product,year,grid_kw,min_r", HARP_CASES,
                             ids=[c[0].strip("_") for c in HARP_CASES])
    @pytest.mark.parametrize("month", ["03", "07", "12"])
    def test_vs_harp_oracle(self, product, year, grid_kw, min_r, month):
        """自建超取樣 vs HARP bin_spatial:逐格 r 不得低於該產品門檻,偏差 ±1% 內。"""
        from src.processing.l3.harp_oracle import harp_available, harp_oracle
        from src.processing.l3.runner import make_adapter
        if not harp_available():
            pytest.skip("HARP CLI 未安裝(conda-forge 的 harp;設 HARPCONVERT 或讓 harpconvert 在 PATH 上)")
        fs = _glob_roots(f"Sentinel-5P/raw/L2/{product}/{year}/{month}/*.nc")
        if not fs:
            pytest.skip(f"找不到 {product} {year}/{month} 的樣本檔")
        grid = GridSpec(**grid_kw)                      # 用 km 網格對齊 HARP 驗證慣例
        oracle = harp_oracle(fs[0], product, grid)
        if oracle is None:
            pytest.skip("HARP 未產出 oracle(該軌可能無有效資料)")
        g = make_adapter("s5p", product).read(fs[0])
        if g is None:
            pytest.skip("adapter 讀不到有效資料")
        gf = SupersampleBinRegridder(K=4).regrid(g, grid)
        m = compare_fields(oracle, gf.value)
        assert m["n_common"] > 50, f"{product} {year}/{month}: 共同有效格只有 {m['n_common']}"
        assert m["cell_r"] >= min_r, f"{product} {year}/{month}: r={m['cell_r']:.4f} < {min_r}"
        assert abs(m["cell_bias_pct"]) < 1.0, f"{product} {year}/{month}: bias={m['cell_bias_pct']:.2f}%"
