"""MODIS 原始檔的結構性事實,寫成會持續生效的檢查。

取代 `wip_coverage/` 那幾支一次性的 print 腳本(`check_modis_resolution.py`、
`check_daily_duplicates.py`、`analyze_tile_coverage.py`、`test_specific_tiles.py`)。
那些腳本把結論印在終端機上、看過就沒了 —— **`Optical_Depth_047` 是 (orbit, y, x) 這件事
當初就印出來過,但沒有任何東西擋著「只取第 0 層」的寫法**,所以那個丟掉 165% 資料的 bug
一直活著。事實變成斷言之後才擋得住。

全部需要真實 HDF + pyhdf,碟沒掛就 skip。
"""
from __future__ import annotations

import glob
from pathlib import Path

import numpy as np
import pytest

MCD19A2_GLOB = "/Volumes/Transcend/MODIS/raw/MCD19A2/*/*/*.hdf"
TAIWAN_TILES = ("h28v06", "h29v06")     # 覆蓋台灣需要的兩塊 sinusoidal tile


def _samples(n=6):
    fs = [f for f in sorted(glob.glob(MCD19A2_GLOB)) if "/._" not in f]
    if not fs:
        pytest.skip("找不到 MCD19A2 原始檔(外接碟未掛載?)")
    return fs[:n]


def _sd(path):
    pytest.importorskip("pyhdf", reason="需要 pyhdf([ingest] extra)")
    from pyhdf.SD import SD, SDC
    return SD(str(path), SDC.READ)


@pytest.mark.requires_data
class TestMCD19A2RawStructure:
    def test_aod_is_orbit_stacked(self):
        """`Optical_Depth_047` 是 (orbit, y, x) —— 一天多次過境,不是單張影像。

        這是「只取第 0 層會丟資料」的根本原因,所以把它釘成斷言。
        """
        multi = 0
        for f in _samples():
            h = _sd(f)
            try:
                if "Optical_Depth_047" not in h.datasets():
                    continue
                a = np.asarray(h.select("Optical_Depth_047")[:])
                assert a.ndim == 3, f"{Path(f).name}: 預期 3D,實得 {a.shape}"
                if a.shape[0] > 1:
                    multi += 1
            finally:
                h.end()
        assert multi > 0, "所有樣本都只有單一軌道層,與已知結構不符"

    def test_tile_is_1200_square(self):
        """MAIAC 1km tile = 1200×1200(空間兩維)。"""
        for f in _samples(3):
            h = _sd(f)
            try:
                if "Optical_Depth_047" not in h.datasets():
                    continue
                a = np.asarray(h.select("Optical_Depth_047")[:])
                assert a.shape[-2:] == (1200, 1200), f"{Path(f).name}: {a.shape}"
            finally:
                h.end()

    def test_adapter_uses_all_orbits_not_just_the_first(self):
        """回歸:adapter 的有效點數必須是**跨軌道聯集**,不是第 0 層。

        實測 8 檔:第 0 層 327k 點、聯集 866k 點。只要有人把 keep_orbits 拿掉,
        這條就會失敗。
        """
        from src.processing.l3.adapters.modis import MODISAdapter
        adapter = MODISAdapter("MCD19A2")
        checked = 0
        for f in _samples(6):
            h = _sd(f)
            try:
                if "Optical_Depth_047" not in h.datasets():
                    continue
                d = h.select("Optical_Depth_047")
                raw = np.asarray(d[:])
                fill = d.attributes().get("_FillValue", -28672)
            finally:
                h.end()
            if raw.ndim != 3 or raw.shape[0] < 2:
                continue
            valid = raw != fill
            first, union = int(valid[0].sum()), int(valid.any(axis=0).sum())
            if union <= first:            # 這個檔第 0 層剛好已是全部,測不出差異
                continue
            g = adapter.read(f)
            if g is None:
                continue
            got = int(np.isfinite(g.values).sum())
            assert got > first, (f"{Path(f).name}: adapter 只拿到 {got} 點,"
                                 f"第 0 層有 {first}、聯集有 {union} → 疑似退回只取第一層")
            checked += 1
        if checked == 0:
            pytest.skip("樣本中沒有「聯集 > 第 0 層」的檔,無法驗證")

    def test_taiwan_needs_two_tiles(self):
        """台灣跨兩塊 tile;少一塊就會缺半邊。"""
        fs = [f for f in sorted(glob.glob(MCD19A2_GLOB)) if "/._" not in f]
        if not fs:
            pytest.skip("找不到 MCD19A2 原始檔")
        found = {t for t in TAIWAN_TILES if any(t in Path(f).name for f in fs)}
        assert found == set(TAIWAN_TILES), f"缺 tile: {set(TAIWAN_TILES) - found}"

    def test_same_day_tiles_are_unioned_not_duplicated(self):
        """同一天的多個 tile 必須被聚合成**一天**,不是各自成一筆。"""
        from src.processing.l3 import (GridSpec, L3Pipeline, L3Writer,
                                       SupersampleBinRegridder)
        from src.processing.l3.adapters import MODISAdapter
        fs = [f for f in sorted(glob.glob(MCD19A2_GLOB)) if "/._" not in f][:6]
        if len(fs) < 2:
            pytest.skip("樣本不足")
        # 檔名 .AYYYYDDD. → 同日的檔案
        def doy(p):
            return Path(p).name.split(".")[1]
        days = {doy(f) for f in fs}
        grid = GridSpec.from_degrees(0.05, (119.0, 123.0, 21.0, 26.0))
        pipe = L3Pipeline(MODISAdapter("MCD19A2"), SupersampleBinRegridder(K=2),
                          grid, L3Writer())
        res = pipe.aggregate(fs, freq="D")
        assert len(res) <= len(days), (f"聚合出 {len(res)} 窗,但輸入只有 {len(days)} 個不同日期"
                                       " → 同日多 tile 被重複計為不同期別")
