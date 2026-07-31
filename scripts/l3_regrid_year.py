#!/usr/bin/env python3
"""用統一 L3 pipeline 把某產品某年的 raw 重新網格化成**單一目標網格**的全年檔。

取代舊的「逐軌 RBF 內插 → merge」路徑:改走 footprint 超取樣 binning
(``SupersampleBinRegridder``,vs HARP oracle r≈0.999),並在**指定的精確度數網格**
上聚合成逐日場,直接輸出模型可讀的 ``(time, lat, lon)``。

用法:
    python -m scripts.l3_regrid_year --source s5p --product NO2___ --year 2024
    python -m scripts.l3_regrid_year --source modis --product MCD19A2 --year 2023
    python -m scripts.l3_regrid_year --source s5p --product SO2___ --year 2024 --deg 0.02 --dry-run

設計取捨:
- 預設 ``--deg 0.02`` = 模型共同網格(251×201),輸出**不需要**再 coarsen/interp/rename。
- 輸出檔名帶 ``l3_002deg`` 標記,**不會蓋掉**任何既有檔。
- ``--freq D`` 逐日聚合(同日多軌加權平均);``M``/``Y`` 亦可。
"""
from __future__ import annotations

import argparse
import glob
import os
import sys
import time
from pathlib import Path

import numpy as np

from src.processing.l3 import (GridSpec, L3Pipeline, L3Writer,
                               SupersampleBinRegridder)

BOUNDS = (119.0, 123.0, 21.0, 26.0)
LOCAL_WORK = Path("/Users/chanchihyu/Satellite/Data")

# source → (raw glob 樣板, adapter 工廠, 輸出短變數名)
SHORT_NAME = {"NO2___": "no2", "O3____": "o3", "SO2___": "so2", "HCHO__": "hcho",
              "MCD19A2": "aod", "MOD04_L2": "aod", "MYD04_L2": "aod"}
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
            pats.append(str(b / "GEMS" / "raw" / product / str(year) / "*" / "*.nc"))
    out = []
    for p in pats:
        out += [Path(f) for f in glob.glob(p) if not Path(f).name.startswith("._")]
    return sorted(set(out))


def _make_adapter(source: str, product: str):
    if source == "s5p":
        from src.processing.l3 import S5PAdapter
        return S5PAdapter(product)
    if source == "modis":
        from src.processing.l3 import MODISAdapter
        return MODISAdapter(product)
    if source == "gems":
        from src.processing.l3 import GEMSAdapter
        return GEMSAdapter(product)
    raise ValueError(f"未知 source: {source}")


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True, choices=["s5p", "modis", "gems"])
    ap.add_argument("--product", required=True, help="S5P: NO2___/O3____/SO2___;MODIS: MCD19A2")
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--deg", type=float, default=0.02, help="目標網格度數(預設 0.02 = 模型網格)")
    ap.add_argument("--freq", default="D", choices=["D", "M", "Y"])
    ap.add_argument("--K", type=int, default=4, help="超取樣每邊子點數")
    ap.add_argument("--qa", type=float, default=0.5, help="S5P qa_value 門檻")
    ap.add_argument("--out", default=None)
    ap.add_argument("--limit", type=int, default=None, help="只處理前 N 檔(測試用)")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)

    base_dirs = [Path("/Volumes/Transcend"), Path("/Volumes/TOSHIBA")]
    base_dirs = [b for b in base_dirs if b.exists()]
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
    print(f"[l3] 目標網格 {a.deg}° bounds={BOUNDS}  freq={a.freq}  K={a.K}", flush=True)
    print(f"[l3] 輸出 → {out}", flush=True)
    if a.dry_run:
        print("[l3] --dry-run,結束。")
        return 0

    grid = GridSpec.from_degrees(a.deg, BOUNDS)
    adapter = _make_adapter(a.source, a.product)
    regridder = SupersampleBinRegridder(K=a.K, qa_threshold=a.qa)
    pipe = L3Pipeline(adapter, regridder, grid, L3Writer())

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

    res = pipe.aggregate(files, freq=a.freq, progress=progress)
    if not res:
        print("[l3] 聚合結果為空(所有 granule 都無有效資料)", file=sys.stderr)
        return 1

    periods = [p for p, _ in res]
    results = [r for _, r in res]
    L3Writer().write_series(
        periods, results, grid, adapter.product, out,
        short_name=SHORT_NAME.get(a.product),
        extra_attrs={"source": a.source, "product": a.product, "year": str(a.year),
                     "n_source_files": str(len(files)),
                     "n_skipped": str(done["skip"]),
                     "qa_threshold": str(a.qa), "supersample_K": str(a.K)},
    )
    cov = np.mean([np.isfinite(r["value"]).mean() for r in results]) * 100
    print(f"[l3] 完成:{len(res)} 個 {a.freq} 窗,平均逐窗覆蓋 {cov:.1f}%,"
          f"耗時 {time.time()-t0:.0f}s → {out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
