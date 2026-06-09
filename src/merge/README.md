# src.merge — 把 processed 網格沿時間軸合併成單一 cube

各 hub 的 processor 已把每個 granule 內插到固定 lat/lon,所以「合併」只是:
依 product/日期範圍找出 processed nc → 給每檔真實 `time` 座標 → 沿 `time` concat
→ 壓縮寫出一個 `(time, lat, lon)` nc。

探索沿用 `src.coverage` 的 registry/reader(同一套 per-hub 知識);Sentinel-5P **L3**
(`processed/L3/<product>/<aggregation>/`)結構不同,於本套件直接探索。

## 支援(v1)
| hub | processed 來源 | time 來源 |
|---|---|---|
| `sentinel5p`(L2) | `processed/L2/<PRODUCT>/Y/M/*.nc` | 檔內 datetime64(每軌一檔,同日多軌→time 重複,正常) |
| `sentinel5p`(L3) | `processed/L3/<product>/<agg>/*.nc`(需 `--level L3 --aggregation`) | 檔名日期(檔內 time 只是 index) |
| `gems` | `processed/<PRODUCT>/Y/M/*.nc` | 檔內 datetime64(逐時) |
| `modis` | `processed/<PRODUCT>/*.nc`(已是整年 cube) | 檔內 datetime64(跨年 concat) |

ERA5 是逐站 CSV(非網格)→ 不適用。

## 用法
```python
from src.merge import merge_product
merge_product("gems", "NO2", "2022-01-01", "2023-12-31")                 # 寫到 processed/
merge_product("sentinel5p", "no2-tropospheric", "2022-01-01", "2023-12-31",
              level="L3", aggregation="day")                             # S5P L3
ds = merge_product("modis", "MCD19A2", "2022-01-01", "2024-12-31", return_dataset=True)
```
```bash
python -m src.merge --hub sentinel5p --product NO2___ --start 2023-01-01 --end 2023-12-31
python -m src.merge --hub sentinel5p --product no2-tropospheric --level L3 \
    --aggregation day --start 2022-01-01 --end 2023-12-31
```
預設輸出到該 hub 的 `processed/<Dir>_<product>_merged_<range>.nc`(L3 放 `processed/L3/<product>/`);
`--out` 可覆寫,`--base-dir` 覆寫碟位(GEMS/L3 在 TOSHIBA、S5P-L2/MODIS 在 Transcend/DataCenter)。
