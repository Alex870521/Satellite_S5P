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
- ``--freq granule`` 逐軌:每個 granule 一個時間步(時間 = 過境時刻,不合併同日多軌),
  給需要「單次過境 + 當下風場」的分析(如羽流旋轉)。預設檔名加 ``_granule``,不會蓋到逐日年檔。
"""
from __future__ import annotations


import argparse
import glob
import sys
import time
from pathlib import Path


from src.config.settings import DATA_ROOTS, LOCAL_WORK_DIR
from src.processing.l3.runner import (DEFAULT_BOUNDS, GEMS_RAW_DIR, SHORT_NAME, gems_slot,
                                      regrid_to_series)

# 資料根目錄(可跨碟,見 settings.DATA_ROOTS);測試用 monkeypatch 換掉
BASE_DIRS = list(DATA_ROOTS)
# 本機放 gridded 工作檔的位置。可用 LOCAL_WORK_DIR 覆寫（換機器不必改碼）。
LOCAL_WORK = LOCAL_WORK_DIR

# source → (raw glob 樣板, adapter 工廠, 輸出短變數名)
PREFIX = {"s5p": "S5P", "modis": "MODIS", "gems": "GEMS"}

# GEMS 的預設(2026-10-06,沿用 GEMS×TROPOMI 比對研究):
#   雲量 ≤ 0.3(官方建議)、DOAS 擬合殘差 ≤ 0.005(擋 24.34°N 探測器壞列,FinalAlgorithmFlags 擋不到);
#   依台灣當地日期分組(UTC 23:45 是隔天 07:45);每個時槽各一個年檔,不把日變化平均掉。
GEMS_DEFAULTS = {"cloud_max": 0.3, "rms_max": 0.005, "tz_offset_hours": 8, "slot_mode": "per-slot"}
# MCD19A2:550 nm + AOD_QA best(與 catalog 標示一致)。要重現舊 0.01° 年檔口徑用 --aod-band 047 --aod-qa none
MCD19A2_DEFAULTS = {"aod_band": "055", "aod_qa": "best"}
# 檔名用的產品標籤:MODIS 三個產品的變數短名都是 aod,檔名必須分開,否則互相覆蓋
FILE_TAG = {"MCD19A2": "mcd19a2_aod", "MOD04_L2": "mod04_aod", "MYD04_L2": "myd04_aod"}


_UNSET = object()   # 「使用者沒給」;argparse 會對字串預設值套 type,所以不能用字串


def _opt_float(v: str) -> float | None:
    """argparse 用:'none' → None(關閉該篩選)。"""
    return None if v.lower() == "none" else float(v)


def _discover(source: str, product: str, year: int, base_dirs: list[Path]) -> list[Path]:
    """在 SATELLITE_DATA_ROOTS 的每一顆碟上找 raw 檔(同一產品可能分散在多顆碟)。"""
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
    ap.add_argument("--freq", default="D", choices=["D", "M", "Y", "granule"],
                    help="D/M/Y 時間聚合;granule = 逐軌(每個 granule 一個時間步,時間為過境時刻)")
    ap.add_argument("--K", type=int, default=4, help="超取樣每邊子點數")
    ap.add_argument("--qa", type=float, default=0.5, help="S5P qa_value 門檻")
    ap.add_argument("--out", default=None)
    ap.add_argument("--limit", type=int, default=None, help="只處理前 N 檔(測試用)")
    ap.add_argument("--dry-run", action="store_true")
    g = ap.add_argument_group("GEMS 專用(其他衛星忽略)")
    g.add_argument("--cloud-max", type=_opt_float, default=_UNSET,
                   help=f"CloudFraction 上限;'none' 關閉(GEMS 預設 {GEMS_DEFAULTS['cloud_max']})")
    g.add_argument("--rms-max", type=_opt_float, default=_UNSET,
                   help=f"DOAS 擬合殘差上限;'none' 關閉(GEMS 預設 {GEMS_DEFAULTS['rms_max']})")
    g.add_argument("--slot-mode", choices=["per-slot", "merge"], default=None,
                   help="per-slot:每個時槽各一個年檔(GEMS 預設);merge:所有時槽合成一個")
    g.add_argument("--slots", default=None, help="只處理這些時槽(UTC HHMM,逗號分隔),例 0445,0545")
    m = ap.add_argument_group("MCD19A2 專用(其他產品忽略)")
    m.add_argument("--aod-band", choices=["047", "055"], default=MCD19A2_DEFAULTS["aod_band"],
                   help="Optical_Depth_047(470 nm)或 _055(550 nm,預設)")
    m.add_argument("--aod-qa", choices=["none", "best"], default=MCD19A2_DEFAULTS["aod_qa"],
                   help="best(預設)= AOD_QA 雲遮罩 clear 且品質 best;none = 不篩")
    ap.add_argument("--tz-offset", type=float, default=None,
                    help="分組日期的時區位移(小時)。GEMS 預設 8 = 台灣當地日期;其他預設 0 = UTC")
    a = ap.parse_args(argv)

    gems = a.source == "gems"
    adapter_kwargs: dict = {}
    if gems:
        adapter_kwargs = {"cloud_max": GEMS_DEFAULTS["cloud_max"] if a.cloud_max is _UNSET else a.cloud_max,
                          "rms_max": GEMS_DEFAULTS["rms_max"] if a.rms_max is _UNSET else a.rms_max}
    if a.source == "modis" and a.product == "MCD19A2":
        adapter_kwargs = {"aod_band": a.aod_band, "aod_qa": a.aod_qa}
    tz = a.tz_offset if a.tz_offset is not None else (GEMS_DEFAULTS["tz_offset_hours"] if gems else 0)
    slot_mode = a.slot_mode or (GEMS_DEFAULTS["slot_mode"] if gems else "merge")

    base_dirs = [b for b in BASE_DIRS if b.exists()]
    if not base_dirs:
        print("SATELLITE_DATA_ROOTS 裡沒有任何存在的資料根目錄", file=sys.stderr)
        return 2
    files = _discover(a.source, a.product, a.year, base_dirs)
    if a.limit:
        files = files[: a.limit]
    print(f"[l3] {a.source}/{a.product} {a.year}: 找到 {len(files)} 個 raw 檔", flush=True)
    if not files:
        print("[l3] 沒有 raw 可處理 — 該年的原始檔可能已被刪除。", file=sys.stderr)
        return 1

    if gems and a.slots:
        want = {x.strip() for x in a.slots.split(",") if x.strip()}
        files = [f for f in files if gems_slot(f.name) in want]
        print(f"[l3] 限定時槽 {sorted(want)}:剩 {len(files)} 檔", flush=True)

    tag = f"{a.deg:g}".replace("0.", "").replace(".", "")   # 0.02 -> 02
    out = Path(a.out) if a.out else (
        LOCAL_WORK / "l3" / f"{PREFIX[a.source]}_{FILE_TAG.get(a.product) or SHORT_NAME.get(a.product, a.product)}"
                     f"_l3_{tag}deg_{a.year}{'' if a.freq == 'D' else '_' + a.freq}.nc")

    # 每個工作 = (該批檔案, 輸出路徑, 時槽)。per-slot 時檔名加上 _HHMMUTC。
    jobs: list[tuple[list[Path], Path, str | None]]
    if gems and slot_mode == "per-slot":
        by_slot: dict[str, list[Path]] = {}
        for f in files:
            sl = gems_slot(f.name)
            if sl:
                by_slot.setdefault(sl, []).append(f)
        jobs = [(fs, out.with_name(f"{out.stem}_{sl}UTC{out.suffix}"), sl) for sl, fs in sorted(by_slot.items())]
        n_lost = len(files) - sum(len(fs) for fs, _, _ in jobs)
        if n_lost:
            print(f"[l3] ⚠️ {n_lost} 個檔讀不出時槽(檔名不是 GK2_GEMS_L2_YYYYMMDD_HHMM_…),已略過", file=sys.stderr)
        if not jobs:
            print("[l3] 沒有任何檔讀得出時槽,per-slot 模式無事可做", file=sys.stderr)
            return 1
    else:
        jobs = [(files, out, None)]

    print(f"[l3] 目標網格 {a.deg}° bounds={DEFAULT_BOUNDS}  freq={a.freq}  K={a.K}"
          f"  日期基準 {'UTC' if not tz else f'UTC{tz:+g}h'}"
          + (f"  品質篩選 {adapter_kwargs}" if adapter_kwargs else ""), flush=True)
    for fs, o, sl in jobs:
        print(f"[l3] 輸出 → {o}" + (f"({len(fs)} 檔)" if sl else ""), flush=True)
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

    rc = 0
    for fs, o, sl in jobs:
        attrs = {"year": str(a.year)}
        if sl:
            attrs["gems_slot_utc"] = sl
        try:
            stats = regrid_to_series(
                a.source, a.product, fs, o,
                deg=a.deg, bounds=DEFAULT_BOUNDS, freq=a.freq, K=a.K, qa=a.qa,
                short_name=SHORT_NAME.get(a.product),
                extra_attrs=attrs,
                progress=progress,
                adapter_kwargs=adapter_kwargs,
                tz_offset_hours=tz,
            )
        except ValueError as exc:                       # 聚合結果為空
            print(f"[l3] {o.name}:{exc}", file=sys.stderr)
            rc = 1
            continue
        print(f"[l3] 完成:{stats['n_periods']} 個 {a.freq} 窗,平均逐窗覆蓋 {stats['mean_coverage']:.1f}%,"
              f"略過 {stats['n_skipped']} 檔,耗時 {stats['seconds']:.0f}s → {stats['out']}", flush=True)
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
