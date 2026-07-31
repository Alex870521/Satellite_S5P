#!/usr/bin/env python3
"""
下載 MCD19A2 (MAIAC AOD) 並處理成全年 gridded nc（模型用）。

MODIS hdf 很小（~5MB/檔、~4.6GB/年），下載到 Transcend MODIS raw，
再用 merge_hdf_files_to_netcdf（cKDTree 修過、不卡死）產全年 0.01° 檔到本機。

用法：
  python -m scripts.download_process_aod --year 2021
  python -m scripts.download_process_aod --year 2025 --start-month 7   # 只補 7–12 月再處理全年
"""
from __future__ import annotations
import argparse
import logging
import shutil
from datetime import datetime
from pathlib import Path

import xarray as xr
import numpy as np

from src.api import MODISHub
from src.processing.modis_processor import MODISProcessor

# earthaccess bounding_box 順序：(min_lon, min_lat, max_lon, max_lat)
BOUNDARY = (119, 21, 123, 26)
LOCAL = Path("/Users/chanchihyu/Satellite/Data")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--start-month", type=int, default=1)
    ap.add_argument("--end-month", type=int, default=12)
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    y = a.year

    # 1) 下載缺的月份（earthaccess 會跳過已存在的檔）
    print(f"[dl] MCD19A2 {y}-{a.start_month:02d} ~ {y}-{a.end_month:02d} …")
    hub = MODISHub()
    last_day = 31 if a.end_month in (1, 3, 5, 7, 8, 10, 12) else (
        30 if a.end_month != 2 else 28)
    prods = hub.fetch_data(file_type="MCD19A2",
                           start_date=datetime(y, a.start_month, 1),
                           end_date=datetime(y, a.end_month, last_day),
                           boundary=BOUNDARY)
    print(f"[dl] 搜到 {len(prods) if prods else 0} 個 granule，開始下載 …")
    if prods:
        hub.download_data(prods)

    # 2) 處理全年 → gridded nc（不論下載範圍，merge 整年現有 raw）
    print(f"[grid] merge {y} 全年 raw → 0.01° nc …")
    p = MODISProcessor()
    p.raw_dir = hub.raw_dir
    p.processed_dir = LOCAL / f"_modis_tmp{y}"
    p.figure_dir = Path("/tmp/modis_fig")
    p.file_type = "MCD19A2"
    p.logger = logging.getLogger("modis")
    ok = p.merge_hdf_files_to_netcdf(
        start_date=f"{y}-01-01", end_date=f"{y}-12-31",
        merge_by_month=False, output_filename=f"MCD19A2_{y}0101_{y}1231")

    src = LOCAL / f"_modis_tmp{y}" / "MCD19A2" / f"MCD19A2_{y}0101_{y}1231.nc"
    dst = LOCAL / f"MCD19A2_{y}0101_{y}1231.nc"
    if ok and src.exists():
        shutil.move(str(src), str(dst))
        shutil.rmtree(LOCAL / f"_modis_tmp{y}", ignore_errors=True)
        ds = xr.open_dataset(dst)
        print(f"[done] {dst.name} {dict(ds.sizes)} 有效率 "
              f"{np.isfinite(ds.aod.values).mean()*100:.1f}%")
    else:
        print(f"[FAILED] ok={ok} src_exists={src.exists()}")


if __name__ == "__main__":
    main()
