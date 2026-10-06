"""GEMS 產 L3 年檔前的三個修正(2026-10-06):

1. 品質篩選:GEMS 預設套用比對研究的值 —— 雲量 ≤ 0.3、DOAS 擬合殘差 ≤ 0.005(擋 24.34°N 壞列)。
2. 時槽:GEMS 一天 9 個時槽,預設每個時槽各出一個年檔,不把日變化平均掉。
3. 日期:GEMS 依台灣當地日期(UTC+8)分組;UTC 23:45 是隔天早上 07:45,不能算進前一天。
S5P / MODIS 的行為一律不變。全部不需外接碟。
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

GEMS_ARGS = ["--source", "gems", "--product", "GEMS_NO2_TROP", "--year", "2022"]


def _names(tmp_path, slots=("0445", "0545"), days=(1, 2)):
    return [tmp_path / f"GK2_GEMS_L2_202201{d:02d}_{s}_NO2_FW_DPRO_ORI.nc" for d in days for s in slots]


class TestLocalDate:
    def test_2345_utc_belongs_to_next_local_day(self):
        from src.processing.l3.pipeline import _period_key
        t = np.datetime64("2022-01-01T23:45", "ns")
        assert _period_key(t, "D") == np.datetime64("2022-01-01")
        assert _period_key(t, "D", tz_offset_hours=8) == np.datetime64("2022-01-02")

    def test_midday_slot_same_day_either_way(self):
        from src.processing.l3.pipeline import _period_key
        t = np.datetime64("2022-01-01T04:45", "ns")
        assert _period_key(t, "D") == _period_key(t, "D", tz_offset_hours=8)


class TestSlotsAndAdapterOptions:
    def test_slot_from_filename(self):
        from src.processing.l3.runner import gems_slot
        assert gems_slot("GK2_GEMS_L2_20220101_0445_NO2_FW_DPRO_ORI.nc") == "0445"
        assert gems_slot("GK2_GEMS_L2_20220101_2345_NO2_HE-ETC_DPRO_ORI.nc") == "2345"
        assert gems_slot("not_a_gems_file.nc") is None

    def test_make_adapter_passes_qc_options(self):
        from src.processing.l3.runner import make_adapter
        ad = make_adapter("gems", "GEMS_NO2_TROP", cloud_max=0.3, rms_max=0.005)
        assert (ad.cloud_max, ad.rms_max) == (0.3, 0.005)
        assert make_adapter("gems", "GEMS_NO2_TROP").cloud_max is None   # 直接呼叫時預設不變


class TestCli:
    def _run(self, monkeypatch, tmp_path, files, argv):
        import scripts.l3_regrid_year as cli
        monkeypatch.setattr(cli, "BASE_DIRS", [tmp_path])
        monkeypatch.setattr(cli, "_discover", lambda *a, **k: files)
        monkeypatch.setattr(cli, "LOCAL_WORK", tmp_path)
        calls = []

        def fake(source, product, fs, out_path, **kw):
            calls.append(dict(files=list(fs), out=Path(out_path), **kw))
            return {"n_files": len(fs), "n_periods": 1, "n_skipped": 0, "mean_coverage": 1.0,
                    "out": Path(out_path), "seconds": 0.0}
        monkeypatch.setattr(cli, "regrid_to_series", fake)
        assert cli.main(argv) == 0
        return calls

    def test_gems_defaults_per_slot_with_study_qc_and_local_date(self, tmp_path, monkeypatch):
        files = _names(tmp_path)
        calls = self._run(monkeypatch, tmp_path, files, GEMS_ARGS)
        assert len(calls) == 2                                   # 每個時槽一個檔
        slots = sorted(c["out"].name for c in calls)
        assert slots[0].endswith("_2022_0445UTC.nc") and slots[1].endswith("_2022_0545UTC.nc")
        for c in calls:
            slot = c["out"].stem.split("_")[-1][:4]
            assert all(f"_{slot}_" in f.name for f in c["files"])   # 只含該時槽的檔
            assert c["adapter_kwargs"] == {"cloud_max": 0.3, "rms_max": 0.005}
            assert c["tz_offset_hours"] == 8
            assert c["extra_attrs"]["gems_slot_utc"] == slot

    def test_gems_slot_filter_and_merge_mode(self, tmp_path, monkeypatch):
        files = _names(tmp_path, slots=("0345", "0445", "0545"))
        calls = self._run(monkeypatch, tmp_path, files, GEMS_ARGS + ["--slots", "0445,0545", "--slot-mode", "merge"])
        assert len(calls) == 1
        assert {f.name.split("_")[4] for f in calls[0]["files"]} == {"0445", "0545"}

    def test_gems_qc_can_be_disabled(self, tmp_path, monkeypatch):
        calls = self._run(monkeypatch, tmp_path, _names(tmp_path, slots=("0445",)),
                          GEMS_ARGS + ["--cloud-max", "none", "--rms-max", "none"])
        assert calls[0]["adapter_kwargs"] == {"cloud_max": None, "rms_max": None}

    def test_s5p_behaviour_unchanged(self, tmp_path, monkeypatch):
        files = [tmp_path / "S5P_OFFL_L2__NO2____20220101T042839_20220101T061009_21846_02_020301_20220101T203203.nc"]
        calls = self._run(monkeypatch, tmp_path, files, ["--source", "s5p", "--product", "NO2___", "--year", "2022"])
        assert len(calls) == 1
        assert calls[0]["adapter_kwargs"] == {} and calls[0]["tz_offset_hours"] == 0
        assert calls[0]["out"].name == "S5P_no2_l3_02deg_2022.nc"
