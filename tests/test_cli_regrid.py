"""C9:``scripts/l3_regrid_year.py`` 必須走 ``runner.regrid_to_series``,不能自己再寫一份編排。

B5 的問題是 runner docstring 說「CLI 與 process_l3 都走這裡」但 CLI 其實從沒呼叫它。
這裡三個結構測試不需外接碟;golden 測試用 2026-08-05 重構前同一指令實跑的數字,有碟才跑。
"""
from __future__ import annotations

import inspect
from pathlib import Path

import numpy as np
import pytest
import xarray as xr

ARGS = ["--source", "gems", "--product", "GEMS_NO2_TROP", "--year", "2022"]


class TestCliDelegatesToRunner:
    def test_main_calls_regrid_to_series_with_cli_args(self, tmp_path, monkeypatch):
        import scripts.l3_regrid_year as cli
        fake_files = [tmp_path / f"GK2_GEMS_L2_202201{d:02d}_0445_NO2_FW_DPRO_ORI.nc" for d in (1, 2, 3)]
        monkeypatch.setattr(cli, "BASE_DIRS", [tmp_path])                 # 假裝有碟
        monkeypatch.setattr(cli, "_discover", lambda *a, **k: fake_files)
        seen = {}

        def fake_run(source, product, files, out_path, **kw):
            seen.update(source=source, product=product, files=list(files), out=Path(out_path), **kw)
            return {"n_files": 3, "n_periods": 3, "n_skipped": 0, "mean_coverage": 12.5,
                    "out": Path(out_path), "seconds": 0.1}
        monkeypatch.setattr(cli, "regrid_to_series", fake_run)

        rc = cli.main(ARGS + ["--deg", "0.05", "--freq", "M", "--K", "3", "--qa", "0.7",
                              "--out", str(tmp_path / "o.nc")])
        assert rc == 0
        assert (seen["source"], seen["product"], seen["files"]) == ("gems", "GEMS_NO2_TROP", fake_files)
        assert (seen["deg"], seen["freq"], seen["K"], seen["qa"]) == (0.05, "M", 3, 0.7)
        assert seen["bounds"] == cli.DEFAULT_BOUNDS
        assert seen["short_name"] == "no2_trop"
        assert seen["extra_attrs"]["year"] == "2022"
        assert callable(seen["progress"])

    def test_empty_aggregate_is_reported_not_raised(self, tmp_path, monkeypatch):
        import scripts.l3_regrid_year as cli
        monkeypatch.setattr(cli, "BASE_DIRS", [tmp_path])
        monkeypatch.setattr(cli, "_discover", lambda *a, **k: [tmp_path / "x.nc"])

        def boom(*a, **k):
            raise ValueError("聚合結果為空")
        monkeypatch.setattr(cli, "regrid_to_series", boom)
        # merge:這個測試驗的是「聚合為空要回報」,不是時槽切分
        assert cli.main(ARGS + ["--slot-mode", "merge", "--out", str(tmp_path / "o.nc")]) == 1

    def test_cli_has_no_private_orchestration_left(self):
        """B5 的收斂條件:CLI 裡不能再有自己的 L3Pipeline 組裝或 adapter 工廠副本。"""
        import scripts.l3_regrid_year as cli
        src = inspect.getsource(cli)
        assert "L3Pipeline(" not in src
        assert "_make_adapter" not in src


@pytest.mark.requires_data
class TestCliGolden:
    """重構前(2026-08-05)`--source gems --product GEMS_NO2_TROP --year 2022 --limit 3`
    實跑得到:dims (1,251,201)、finite 0.360、range 2.5e12~2.19e16、var no2_trop。"""

    def test_gems_2022_first3_matches_prerefactor_numbers(self, tmp_path):
        import scripts.l3_regrid_year as cli
        if not any(b.exists() for b in cli.BASE_DIRS):
            pytest.skip("外接碟未掛載")
        out = tmp_path / "g.nc"
        rc = cli.main(ARGS + ["--limit", "3", "--out", str(out),
                                # 釘的是 2026-08-05 的舊語意:不篩雲量/殘差、UTC、時槽合併
                                "--slot-mode", "merge", "--cloud-max", "none",
                                "--rms-max", "none", "--tz-offset", "0"])
        if rc == 1:
            pytest.skip("該碟上找不到 GEMS 2022 raw")
        assert rc == 0
        with xr.open_dataset(out) as ds:
            v = ds["no2_trop"]
            assert dict(ds.sizes) == {"time": 1, "lat": 251, "lon": 201}
            assert abs(float(np.isfinite(v).mean()) - 0.360) < 0.005
            assert abs(float(v.min()) / 2.5e12 - 1) < 0.05
            assert abs(float(v.max()) / 2.19e16 - 1) < 0.01
            assert ds.attrs["year"] == "2022" and "n_skipped" in ds.attrs
