#!/usr/bin/env python3
"""
下載 → 處理 → merge 全年 gridded → 刪除「這次新下載」的 raw（省外接空間）。

安全鐵則（使用者要求）：
  - 只刪「這次新下載、且 merge 驗證成功後」的 raw。
  - **絕不刪除執行前就存在的 raw**（那些要留作其他分析）。
做法：下載前先 snapshot 既有 raw 檔集合；最後只刪「不在 snapshot 內的新檔」，
且僅在全年 merge 檔成功產出並通過驗證後才刪。

用法：
  python -m scripts.fetch_merge_delete_raw --product SO2___ --year 2023
  python -m scripts.fetch_merge_delete_raw --product SO2___ --year 2023 --dry-run   # 只演示不刪
  python -m scripts.fetch_merge_delete_raw --product SO2___ --year 2023 --keep-raw  # 下載+merge 但不刪
"""
from __future__ import annotations
import argparse
import shutil
from datetime import datetime
from pathlib import Path

import xarray as xr

from src.config.settings import BASE_DIR
from src.api import SENTINEL5PHub
from src.merge import merge_product

BOUNDARY = (119, 123, 21, 26)
LOCAL_WORK = Path("/Users/chanchihyu/Satellite/Data")   # 本機放全年 gridded 工作檔


def _raw_dir(product: str) -> Path:
    return BASE_DIR / "Sentinel-5P" / "raw" / "L2" / product


def _snapshot(raw_root: Path, year: int) -> set[Path]:
    """下載前：記下該產品該年既有的 raw 檔（這些絕不刪）。"""
    ydir = raw_root / str(year)
    if not ydir.exists():
        return set()
    return {p for p in ydir.rglob("*.nc") if not p.name.startswith("._")}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--product", required=True, help="S5P 產品碼，如 SO2___ / NO2___ / O3____")
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--file-class", default="OFFL")
    ap.add_argument("--max-workers", type=int, default=3)
    ap.add_argument("--dry-run", action="store_true", help="不下載/不刪，只印計畫")
    ap.add_argument("--keep-raw", action="store_true", help="下載+merge 但不刪 raw")
    a = ap.parse_args()

    y = a.year
    start, end = datetime(y, 1, 1), datetime(y, 12, 31)
    raw_root = _raw_dir(a.product)

    # ---- 1) 安全 snapshot（下載前既有 raw）----
    pre_existing = _snapshot(raw_root, y)
    print(f"[safe] {a.product} {y}: 下載前既有 raw = {len(pre_existing)} 檔（受保護，不會刪）")
    if a.dry_run:
        print("[dry-run] 將：fetch→download→process→merge→（只刪新下載的 raw）。結束。")
        return

    # ---- 2) fetch → download → process ----
    hub = SENTINEL5PHub(max_workers=a.max_workers)
    print(f"[run] fetch+download+process {a.product} {y} …")
    hub.run_pipeline(file_class=a.file_class, file_type=a.product,
                     start_date=start, end_date=end, boundary=BOUNDARY)

    # ---- 3) merge 全年 gridded ----
    print(f"[merge] {a.product} {y} → 全年 nc …")
    out = merge_product("sentinel5p", a.product,
                        f"{y}-01-01", f"{y}-12-31")
    out = Path(out)
    print(f"[merge] 產出 {out}")

    # ---- 4) 驗證 merge 檔（能開、天數合理）----
    ok = False
    try:
        ds = xr.open_dataset(out)
        ndays = ds.sizes.get("time", 0)
        ds.close()
        ok = ndays >= 200          # 全年 S5P 過境扣雲/缺,>=200 視為成功
        print(f"[verify] {out.name}: time={ndays} → {'OK' if ok else '不足，視為失敗'}")
    except Exception as e:
        print(f"[verify] 開檔失敗：{e}")

    # 複製一份全年檔到本機工作目錄（與 2024 命名一致）
    if ok:
        local = LOCAL_WORK / f"S5P_{a.product.strip('_')}_{y}0101_{y}1231.nc"
        shutil.copy2(out, local)
        print(f"[copy] → {local}")

    # ---- 5) 只刪「新下載」的 raw（merge 成功才刪；保護既有）----
    if not ok:
        print("[delete] merge 未通過驗證 → 不刪任何 raw（安全）。")
        return
    if a.keep_raw:
        print("[delete] --keep-raw → 保留所有 raw。")
        return
    now = {p for p in (raw_root / str(y)).rglob("*.nc") if not p.name.startswith("._")}
    to_delete = sorted(now - pre_existing)              # 只刪新檔
    freed = sum(p.stat().st_size for p in to_delete) / 1e9
    print(f"[delete] 將刪 {len(to_delete)} 個新下載 raw（{freed:.1f} GB），既有 {len(pre_existing)} 檔保留")
    for p in to_delete:
        p.unlink(missing_ok=True)
    print(f"[delete] 完成，省下 ~{freed:.1f} GB 外接空間。")


if __name__ == "__main__":
    main()
