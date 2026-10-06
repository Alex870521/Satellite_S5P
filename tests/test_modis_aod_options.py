"""MODIS L3 的兩個修正(2026-10-07):

1. 檔名:MCD19A2 / MOD04_L2 / MYD04_L2 的變數短名都是 aod,CLI 預設檔名以前一樣 → 互相覆蓋。
2. MCD19A2 波段與品質:舊程式讀 Optical_Depth_047(470 nm,標示卻是 550 nm)且沒套 AOD_QA。
   L3 現在預設 550 nm + AOD_QA best;processor 本身的預設維持舊口徑(舊 0.01° 年檔與 CNN 建立在它上面)。
全部不需外接碟(HDF 讀取用假物件代替)。
"""
from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pytest

FILL = -28672


def _qa(cloud, qaod):
    return (np.asarray(qaod) << 8) | np.asarray(cloud)


class _FakeProc:
    """只換掉讀 HDF 的兩個方法,其餘走真的 _extract_mcd19a2_data。"""

    def __init__(self, arrays):
        from src.processing.modis_processor import MODISProcessor
        self.proc = MODISProcessor()
        self.proc.logger = logging.getLogger("test")
        self.proc._get_data_pyhdf = lambda h, name: (arrays[name].copy(), {"scale_factor": 0.001,
                                                                           "_FillValue": FILL})
        self.proc._generate_mcd19a2_coordinates = lambda shape, fn: (np.zeros(shape), np.zeros(shape))
        self.datasets = list(arrays)

    def extract(self, **kw):
        return self.proc._extract_mcd19a2_data(None, self.datasets, "MCD19A2.A2023001.h29v06.hdf",
                                               keep_orbits=True, **kw)[0]


# 一層 2×2:四個像元的 AOD(470 = 550 × 1.2),QA:(clear,best) (clear,鄰雲) (cloudy,best) (clear,best)
A55 = np.array([[[100, 200], [300, FILL]]])
A47 = np.array([[[120, 240], [360, FILL]]])
QA = _qa([[[1, 1], [3, 1]]], [[[0, 3], [0, 0]]])
ARR = {"Optical_Depth_047": A47, "Optical_Depth_055": A55, "AOD_QA": QA}


class TestMcd19a2Extraction:
    def test_processor_default_is_legacy_470_without_qa(self):
        v = _FakeProc(ARR).extract()
        assert np.allclose(v[0, 0], [0.12, 0.24]) and v[0, 1, 0] == pytest.approx(0.36)
        assert np.isnan(v[0, 1, 1])                      # fill 值仍是 NaN

    def test_550_with_best_qa(self):
        v = _FakeProc(ARR).extract(band="055", qa="best")
        assert v[0, 0, 0] == pytest.approx(0.10)          # clear + best → 留
        assert np.isnan(v[0, 0, 1])                       # 鄰雲(QA 3)→ 剔
        assert np.isnan(v[0, 1, 0])                       # 雲遮罩 cloudy → 剔

    def test_asking_550_never_falls_back_to_470(self):
        no55 = {k: v for k, v in ARR.items() if k != "Optical_Depth_055"}
        assert _FakeProc(no55).extract(band="055") is None

    def test_best_qa_without_qa_dataset_skips_file(self):
        noqa = {k: v for k, v in ARR.items() if k != "AOD_QA"}
        assert _FakeProc(noqa).extract(band="055", qa="best") is None

    def test_bad_option_rejected(self):
        with pytest.raises(ValueError):
            _FakeProc(ARR).extract(band="065")


class TestAdapterDefaults:
    def test_l3_adapter_defaults_to_550_best(self):
        from src.processing.l3.runner import make_adapter
        ad = make_adapter("modis", "MCD19A2")
        assert (ad.aod_band, ad.aod_qa) == ("055", "best")
        legacy = make_adapter("modis", "MCD19A2", aod_band="047", aod_qa="none")
        assert (legacy.aod_band, legacy.aod_qa) == ("047", "none")


class TestCliFileNames:
    def _out(self, monkeypatch, tmp_path, product, extra=()):
        import scripts.l3_regrid_year as cli
        monkeypatch.setattr(cli, "BASE_DIRS", [tmp_path])
        monkeypatch.setattr(cli, "_discover", lambda *a, **k: [tmp_path / "x.hdf"])
        monkeypatch.setattr(cli, "LOCAL_WORK", tmp_path)
        seen = {}

        def fake(source, product_, fs, out_path, **kw):
            seen.update(out=Path(out_path), **kw)
            return {"n_files": 1, "n_periods": 1, "n_skipped": 0, "mean_coverage": 1.0,
                    "out": Path(out_path), "seconds": 0.0}
        monkeypatch.setattr(cli, "regrid_to_series", fake)
        assert cli.main(["--source", "modis", "--product", product, "--year", "2023", *extra]) == 0
        return seen

    def test_three_modis_products_never_share_a_file(self, monkeypatch, tmp_path):
        names = {p: self._out(monkeypatch, tmp_path, p)["out"].name
                 for p in ("MCD19A2", "MOD04_L2", "MYD04_L2")}
        assert len(set(names.values())) == 3
        assert names["MCD19A2"] == "MODIS_mcd19a2_aod_l3_02deg_2023.nc"
        assert names["MYD04_L2"] == "MODIS_myd04_aod_l3_02deg_2023.nc"

    def test_mcd19a2_options_reach_adapter(self, monkeypatch, tmp_path):
        assert self._out(monkeypatch, tmp_path, "MCD19A2")["adapter_kwargs"] == \
            {"aod_band": "055", "aod_qa": "best"}
        assert self._out(monkeypatch, tmp_path, "MCD19A2", ["--aod-band", "047", "--aod-qa", "none"]
                         )["adapter_kwargs"] == {"aod_band": "047", "aod_qa": "none"}
        assert self._out(monkeypatch, tmp_path, "MYD04_L2")["adapter_kwargs"] == {}
