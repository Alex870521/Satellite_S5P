#!/usr/bin/env python3
"""用統一 L3 pipeline 把某產品某年的 raw 重新網格化成**單一目標網格**的全年檔。

取代舊的「逐軌 RBF 內插 → merge」路徑:改走 footprint 超取樣 binning
(``SupersampleBinRegridder``,vs HARP oracle r≈0.999),並在**指定的精確度數網格**
上聚合成逐日場,直接輸出模型可讀的 ``(time, lat, lon)``。

用法:
    python -m scripts.l3_regrid_year --source s5p --product NO2___ --year 2024
    python -m scripts.l3_regrid_year --source modis --product MCD19A2 --year 2023
    python -m scripts.l3_regrid_year --source gems  --product GEMS_NO2_TROP --year 2022
    python -m scripts.l3_regrid_year --source s5p --product SO2___ --year 2024 --deg 0.02 --dry-run

設計取捨:
- 預設 ``--deg 0.02`` = 模型共同網格(251×201),輸出**不需要**再 coarsen/interp/rename。
- 輸出檔名帶 ``l3_002deg`` 標記,**不會蓋掉**任何既有檔。
- ``--freq D`` 逐日聚合(同日多軌加權平均);``M``/``Y`` 亦可。
"""
from __future__ import annotations

import os

import argparse
import glob
import sys
import time
from pathlib import Path


from src.config.settings import DATA_ROOTS
from src.processing.l3.runner import (DEFAULT_BOUNDS, GEMS_RAW_DIR, SHORT_NAME,
                                      regrid_to_series)

# 資料根目錄(可跨碟,見 settings.DATA_ROOTS);測試用 monkeypatch 換掉
BASE_DIRS = list(DATA_ROOTS)
# 本機放 gridded 工作檔的位置。可用 LOCAL_WORK_DIR 覆寫（換機器不必改碼）。
LOCAL_WORK = Path(os.getenv("LOCAL_WORK_DIR", Path.home() / "Satellite/Data"))

# source → (raw glob 樣板, adapter 工廠, 輸出短變數名)
PREFIX = {"s5p": "S5P", "modis": "MODIS", "gems": "GEMS"}


def _discover(source: str, product: str, year: int, base_dirs: list[Path]) -> list[Path]:
    """跨碟找 raw 檔(S5P 在 Transcend/TOSHIBA 分散,MODIS 在 Transcend)。"""
    pats = []
    for b in base_dirs:
        if source == "s5p":
            pats.append(str(b / "Sentinel-5P" / "raw" / "L2" / product / str(year) / "*" / "*.nc"))
        elif source == "modis":
            pats.append(str(b / "MODIS" / "raw" / product / str(year) / "*" / "*.hdf"))
            pats.append(str(b / "MODIS" / "raw" / product / str(year) / "*" / "*.nc"))
        elif source == "gems":
            # ⚠️ GEMS 的 --product 是 adapter key(GEMS_NO2_TROP…),不是目錄名;
            # 同一個 NO2 目錄對應三個 key,所以目錄從 GEMS_RAW_DIR 推。
            sub = GEMS_RAW_DIR.get(product)
            if sub is None:
                raise SystemExit(
                    f"GEMS 的 --product 要給 adapter key,可用:{sorted(GEMS_RAW_DIR)};"
                    f"得到 {product!r}(若你想的是目錄 NO2,請改用 GEMS_NO2_TROP / GEMS_NO2 / GEMS_NO2_STRAT)")
            pats.append(str(b / "GEMS" / "raw" / sub / str(year) / "*" / "*.nc"))
    out = []
    for p in pats:
        out += [Path(f) for f in glob.glob(p) if not Path(f).name.startswith("._")]
    return sorted(set(out))


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True, choices=["s5p", "modis", "gems"])
    ap.add_argument("--product", required=True,
                    help="S5P: NO2___/O3____/SO2___;MODIS: MCD19A2;"
                         "GEMS: adapter key GEMS_NO2_TROP / GEMS_NO2 / GEMS_NO2_STRAT / GEMS_O3T"
                         "(目錄自動推,一個 NO2 檔含三個柱量所以用 key 選)")
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--deg", type=float, default=0.02, help="目標網格度數(預設 0.02 = 模型網格)")
    ap.add_argument("--freq", default="D", choices=["D", "M", "Y"])
    ap.add_argument("--K", type=int, default=4, help="超取樣每邊子點數")
    ap.add_argument("--qa", type=float, default=0.5, help="S5P qa_value 門檻")
    ap.add_argument("--out", default=None)
    ap.add_argument("--limit", type=int, default=None, help="只處理前 N 檔(測試用)")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)

    base_dirs = [b for b in BASE_DIRS if b.exists()]
    if not base_dirs:
        print("找不到任何外接碟(Transcend/TOSHIBA)", file=sys.stderr)
        return 2
    files = _discover(a.source, a.product, a.year, base_dirs)
    if a.limit:
        files = files[: a.limit]
    print(f"[l3] {a.source}/{a.product} {a.year}: 找到 {len(files)} 個 raw 檔", flush=True)
    if not files:
        print("[l3] 沒有 raw 可處理 — 該年的原始檔可能已被刪除。", file=sys.stderr)
        return 1

    tag = f"{a.deg:g}".replace("0.", "").replace(".", "")   # 0.02 -> 02
    out = Path(a.out) if a.out else (
        LOCAL_WORK / f"{PREFIX[a.source]}_{SHORT_NAME.get(a.product, a.product)}"
                     f"_l3_{tag}deg_{a.year}.nc")
    print(f"[l3] 目標網格 {a.deg}° bounds={DEFAULT_BOUNDS}  freq={a.freq}  K={a.K}", flush=True)
    print(f"[l3] 輸出 → {out}", flush=True)
    if a.dry_run:
        print("[l3] --dry-run,結束。")
        return 0

    # 編排全部交給 runner.regrid_to_series(B5 收斂):CLI 只負責找檔、命名、印進度。
    t0 = time.time()
    done = {"n": 0, "skip": 0}

    def progress(f, gf):
        done["n"] += 1
        if gf is None:
            done["skip"] += 1
        if done["n"] % 50 == 0:
            el = time.time() - t0
            print(f"  {done['n']}/{len(files)}  略過 {done['skip']}  "
                  f"{el:.0f}s ({el/done['n']:.2f}s/檔)", flush=True)

    try:
        stats = regrid_to_series(
            a.source, a.product, files, out,
            deg=a.deg, bounds=DEFAULT_BOUNDS, freq=a.freq, K=a.K, qa=a.qa,
            short_name=SHORT_NAME.get(a.product),
            extra_attrs={"year": str(a.year)},
            progress=progress,
        )
    except ValueError as exc:                       # 聚合結果為空
        print(f"[l3] {exc}", file=sys.stderr)
        return 1
    print(f"[l3] 完成:{stats['n_periods']} 個 {a.freq} 窗,平均逐窗覆蓋 {stats['mean_coverage']:.1f}%,"
          f"略過 {stats['n_skipped']} 檔,耗時 {stats['seconds']:.0f}s → {stats['out']}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
