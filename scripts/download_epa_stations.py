#!/usr/bin/env python3
"""下載環境部測站逐時值(aqx_p_488),輸出成模型讀得懂的每站 CSV。

輸出到 $MOE_STATION_DIR/<year>/<站名>_aqx_p_488_<起>_<迄>.csv（MOE_STATION_DIR 預設 ./data/stations）
—— 檔名格式與既有年份一致,`cnn/ground.py` 與 `cnn/data.py` 用
`*_aqx_p_488_*.csv` 這個 glob 抓檔,所以日期後綴不影響下游。

⚠️ **`time` 欄要自己補**:API 只給 `datacreationdate`(資料建立時間),
   而既有 CSV 有一個 `time` 欄是「觀測小時」,兩者差一小時
   (例:time=2024-01-01 00:00 對應 datacreationdate=2024-01-01 01:00)。
   下游讀的是 `time`,少了這欄整批資料會被當成沒有時間戳。

⚠️ **TLS**:data.moenv.gov.tw 走 TWCA Global Root CA,那張 root 沒有
   Subject Key Identifier,Python 3.13+ 的嚴格模式會拒絕。這裡用
   `src/utils/tw_gov_tls`(只清掉形式合規檢查、
   保留簽章/效期/主機名驗證,且僅對四個政府網域生效),
   **不要自己改成 verify=False**。

⚠️ **環境部資料庫本身有重複**:同一時間點常出現兩筆。這裡依
   (站名, datacreationdate) 去重,保留第一筆。

需要:`.env` 的 `EPA_API_KEYS`(環境部開放資料平台申請,逗號分隔可放多把,配額用盡自動換下一把)。

用法:
  python -m scripts.download_epa_stations --year 2026 --end-month 8
"""
from __future__ import annotations

import argparse
import calendar
import os
import time as _time
from datetime import datetime, timedelta

import pandas as pd

from src.config.settings import MOE_STATION_DIR   # 也會載入 .env
# 台灣政府網站的 TLS:只放寬 X509_STRICT 這一項格式檢查,**不關驗證**(說明見模組開頭)
from src.utils.tw_gov_tls import requests_session

# 環境部測站逐時檔的位置。可用 MOE_STATION_DIR 覆寫（換機器不必改碼）。
OUT_ROOT = MOE_STATION_DIR
API = "https://data.moenv.gov.tw/api/v2/aqx_p_488"
PAGE = 1000


def _keys() -> list[str]:
    raw = os.getenv("EPA_API_KEYS") or os.getenv("EPA_API_KEY") or ""
    ks = [k.strip() for k in raw.split(",") if k.strip()]
    if not ks:
        raise SystemExit("找不到環境部 API 金鑰:在 .env 設 EPA_API_KEYS(逗號分隔可放多把)")
    return ks


def fetch_day(sess, keys, day: datetime) -> list[dict]:
    """抓某一天的全部測站逐時值。逐日切是為了讓 offset 保持很小 ——
    這個 API 的 offset 一大就開始不穩。"""
    lo = day.strftime("%Y-%m-%d 00:00:00")
    hi = day.strftime("%Y-%m-%d 23:59:59")
    out, offset, ki = [], 0, 0
    while True:
        params = {"api_key": keys[ki], "format": "json", "limit": PAGE,
                  "offset": offset, "sort": "datacreationdate asc",
                  "filters": f"datacreationdate,GR,{lo}|datacreationdate,LE,{hi}"}
        for attempt in range(4):
            try:
                r = sess.get(API, params=params, timeout=120)
                if r.status_code == 200:
                    break
                # 配額用盡就換下一把金鑰
                if r.status_code in (401, 403, 429) and ki + 1 < len(keys):
                    ki += 1
                    params["api_key"] = keys[ki]
                    continue
            except Exception:
                pass
            _time.sleep(2 * (attempt + 1))
        else:
            print(f"  ⚠️ {day:%Y-%m-%d} offset={offset} 連續失敗,略過")
            break
        recs = r.json()
        if not isinstance(recs, list):
            recs = recs.get("records", [])
        out += recs
        if len(recs) < PAGE:
            break
        offset += PAGE
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--start-month", type=int, default=1)
    ap.add_argument("--end-month", type=int, default=12)
    a = ap.parse_args()

    sess = requests_session()
    keys = _keys()

    y = a.year
    start = datetime(y, a.start_month, 1)
    end = datetime(y, a.end_month, calendar.monthrange(y, a.end_month)[1])
    print(f"[epa] {start:%Y-%m-%d} ~ {end:%Y-%m-%d},{len(keys)} 把金鑰")

    rows, day, n_days = [], start, (end - start).days + 1
    while day <= end:
        recs = fetch_day(sess, keys, day)
        rows += recs
        if day.day == 1 or day == end:
            print(f"  {day:%Y-%m-%d}  累計 {len(rows):,} 筆")
        day += timedelta(days=1)

    if not rows:
        print("沒有取得任何資料")
        return 1

    df = pd.DataFrame(rows)
    df["datacreationdate"] = pd.to_datetime(df["datacreationdate"], errors="coerce")
    df = df.dropna(subset=["datacreationdate", "sitename"])
    # ⚠️ 環境部資料庫同一時間點常有兩筆,依 (站名, 建立時間) 去重
    before = len(df)
    df = df.drop_duplicates(subset=["sitename", "datacreationdate"], keep="first")
    # ⚠️ 補回下游要讀的 `time`(觀測小時 = 建立時間 − 1 小時)
    df["time"] = (df["datacreationdate"] - pd.Timedelta(hours=1)).dt.strftime("%Y-%m-%d %H:%M")
    df["datacreationdate"] = df["datacreationdate"].dt.strftime("%Y-%m-%d %H:%M")

    cols = ["time", "datacreationdate", "sitename", "county", "aqi", "pollutant",
            "status", "so2", "co", "o3", "o3_8hr", "pm10", "pm2.5", "no2", "nox",
            "no", "windspeed", "winddirec", "unit", "co_8hr", "pm2.5_avg",
            "pm10_avg", "so2_avg", "longitude", "latitude", "siteid"]
    for c in cols:
        if c not in df.columns:
            df[c] = ""

    out_dir = OUT_ROOT / str(y)
    out_dir.mkdir(parents=True, exist_ok=True)
    tag = f"{start:%Y-%m-%d}_{end:%Y-%m-%d}"
    n_site = 0
    for site, g in df.groupby("sitename"):
        g = g.sort_values("time")[cols]
        # 站名可能含全形括號(如「屏東(枋山)」),檔名照原樣寫,glob 不受影響
        g.to_csv(out_dir / f"{site}_aqx_p_488_{tag}.csv", index=False)
        n_site += 1
    print(f"[epa] 去重 {before:,} → {len(df):,} 筆;寫出 {n_site} 站 → {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
