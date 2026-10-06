#!/usr/bin/env python3
"""
下載某年 ERA5 single-level（模型用 4 檔：blh / u10_v10 / d2m_t2m / r2m）。

ERA5 的「raw」本身就是全年檔（區域裁切、小），模型直接讀、無 merge 步驟。
- blh / u10_v10 / d2m_t2m：cdsapi 下載（每次最多 2 變數，分 3 組）。
- r2m（2m 相對濕度）：由 d2m_t2m 用 Magnus 公式推導（與 2024 同法）。
下載落地 $SATELLITE_BASE_DIR/ERA5,再移進 single_level/<year>/ 子目錄。

用法：SATELLITE_BASE_DIR=/path/to/data \
        python -m scripts.download_era5 --year 2021
"""
from __future__ import annotations

import os
import argparse
import calendar
import shutil
from datetime import datetime
from pathlib import Path

import numpy as np
import xarray as xr

from src.api import ERA5Hub

BOUNDARY = (119, 123, 21, 26)            # (min_lon, max_lon, min_lat, max_lat)
GROUPS = [
    ["boundary_layer_height"],
    ["10m_u_component_of_wind", "10m_v_component_of_wind"],
    ["2m_dewpoint_temperature", "2m_temperature"],
]


def _sat_vp(t_c):
    """飽和水汽壓(hPa)，Magnus。"""
    return 6.112 * np.exp(17.67 * t_c / (t_c + 243.5))


def derive_r2m(d2m_t2m_file: Path, out_file: Path):
    ds = xr.open_dataset(d2m_t2m_file)
    t2m_c = ds["t2m"] - 273.15
    d2m_c = ds["d2m"] - 273.15
    rh = 100.0 * _sat_vp(d2m_c) / _sat_vp(t2m_c)
    rh = rh.clip(0, 100).rename("r2m")
    rh.attrs = {"units": "%", "long_name": "2m relative humidity (derived from d2m,t2m)"}
    rh.to_dataset(name="r2m").to_netcdf(out_file)
    print(f"[r2m] 推導 → {out_file.name}  範圍 {float(rh.min()):.1f}~{float(rh.max()):.1f}%")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--start-month", type=int, default=1)
    ap.add_argument("--end-month", type=int, default=12,
                    help="年份未過完時要指定，否則 CDS 會因為要到未來日期而整個失敗")
    a = ap.parse_args()
    y = a.year
    last_day = calendar.monthrange(y, a.end_month)[1]
    start = datetime(y, a.start_month, 1)
    end = datetime(y, a.end_month, last_day)
    suffix = f"{start:%Y%m%d}_{end:%Y%m%d}"

    hub = ERA5Hub(timezone="Asia/Taipei")
    # hub 下載時自動把檔放進 single_level/<year>/ 子目錄
    ydir = hub.raw_dir / "single_level" / str(y)
    print(f"[era5] 下載 {y} → {ydir}")

    for variables in GROUPS:
        print(f"[era5] 請求 {variables} …（CDS 排隊可能要數分鐘）")
        ok = hub.fetch_data(start_date=start, end_date=end, boundary=BOUNDARY,
                            variables=variables, download_mode="all_at_once")
        if ok:
            hub.download_data()

    # 推導 r2m（從年份子目錄的 d2m_t2m）
    dt = ydir / f"era5_sfc_d2m_t2m_{suffix}.nc"
    if dt.exists():
        derive_r2m(dt, ydir / f"era5_sfc_r2m_{suffix}.nc")
    else:
        print(f"[r2m] 找不到 {dt.name}，跳過推導")

    n = len(list(ydir.glob(f"era5_sfc_*_{suffix}.nc")))
    print(f"[era5] {y} 完成，{ydir} 共 {n} 檔")


if __name__ == "__main__":
    main()
