# src/coverage — 跨衛星「區域 × 時間」資料覆蓋率工具

讀各 hub 的 **processed nc 檔**,依時間維度算出某區域內「有效資料格點 / 區域總格點」的覆蓋率,輸出 tidy 表(可出圖)。離線、不需登入、可重複 re-scan(冪等)。

## 用法

```python
from src.coverage import compute_coverage
df = compute_coverage("sentinel5p", "NO2___", "central",
                      "2023-01-01", "2023-12-31", granularity="monthly")
```

```bash
# 從 repo 根目錄執行
python -m src.coverage --hub sentinel5p --product NO2___ --region central \
    --start 2023-01-01 --end 2023-12-31 --granularity monthly --out cov.csv
```

## 參數

- `--hub`：`sentinel5p sentinel3 gems modis era5 himawari`(別名 s5p/s3)
- `--product`:產品夾名 — S5P `NO2___/O3____/SO2___/HCHO__/CH4___`、MODIS `MYD04_L2`、GEMS `NO2` 等
- `--region`:bbox `taiwan/east_asia`(來自 `settings.REGIONS`)或空品區多邊形 `north/zhumiao/central/yunchianan/kaoping`
- `--granularity`:`per_file`(逐檔診斷)/ `daily`(同日多軌取聯集)/ `monthly` / `yearly`(日覆蓋率平均)
- `--weight`:`count`(格點數)/ `area`(cos 緯度面積權重)

## 輸出欄位

`hub, product, region, time, granularity, weight, valid, total, coverage, n_slices`

## 設計

- **單一通用 reader** `GriddedNCReader` 吃所有規則網格 nc hub(S5P/S3/GEMS/MODIS),差異全在 `registry.HubSpec`。
- **覆蓋率定義**:`valid = isfinite(值) & 在區域內`;`coverage = Σweight(valid) / Σweight(區域)`。processed 已做 QC,NaN 即缺。
- **日為原子單位**:同日多軌/granule 取**聯集**(任一觀測到即算覆蓋),粗粒度再平均。
- **region mask 快取**:同 hub+產品網格固定,point-in-polygon 只算一次。
- ERA5(逐站 CSV)、Himawari(mock)為 stub。

各 hub 原始/處理後檔結構見 [`SCHEMA.md`](SCHEMA.md)。前身原型在 `wip_coverage/`。
