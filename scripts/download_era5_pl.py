"""下載 ERA5 壓力層風(925 + 850 hPa u/v)— 供 PM2.5/NO2 面預測加低層輸送通道.

落地 raw/pressure_level/<year>/era5_pl_u-v_925-850_*.nc。只 fetch+download(不做 per-station CSV)。
用法:repo 根、.venv;python -m scripts.download_era5_pl
"""
import calendar
from src.config import settings  # noqa: load_dotenv() → CDSAPI_URL/KEY 進環境
from src.api import ERA5Hub

BOUNDARY = (119, 123, 21, 26)                # (min_lon, max_lon, min_lat, max_lat)
VARS = ["u_component_of_wind", "v_component_of_wind"]
LEVELS = [925, 850]
YEARS = [2022, 2023, 2024]

if __name__ == "__main__":
    hub = ERA5Hub(timezone="Asia/Taipei")
    ok = fail = 0
    for y in YEARS:
        for mo in range(1, 13):
            last = calendar.monthrange(y, mo)[1]
            tag = f"{y}-{mo:02d}"
            print(f"\n===== ERA5 壓力層 {tag}({LEVELS} hPa) =====", flush=True)
            try:
                hub.fetch_data(start_date=f"{y}-{mo:02d}-01", end_date=f"{y}-{mo:02d}-{last} 23:00",
                               boundary=BOUNDARY, variables=VARS, pressure_levels=LEVELS,
                               download_mode="all_at_once")
                hub.download_data(); ok += 1
            except Exception as e:
                print(f"  {tag} 失敗: {e}", flush=True); fail += 1
    print(f"\nALL DONE  成功 {ok} / 失敗 {fail}", flush=True)
