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

## 畫圖

`plot.py` 吃 `compute_coverage` 的 tidy 表畫**覆蓋率分布直方圖**(每格 `coverage`,0–1),依 `region`/`product` 自動分面板,標 70% 門檻線 + Mean/Median/Std + ≥門檻計數(dpi=600)。

```python
from src.coverage import compute_coverage, plot_coverage_distribution
df = compute_coverage("sentinel5p", "SO2___", "central", "2023-01-01", "2023-12-31")
plot_coverage_distribution(df, "cov_dist.png", threshold=0.7)
```

```bash
# CLI:算覆蓋率順手存分布圖
python -m src.coverage --hub sentinel5p --product SO2___ --region central \
    --start 2023-01-01 --end 2023-12-31 --plot cov_dist.png --threshold 0.7
```

## 代表日地圖(representative days)

依覆蓋率把每天分到 5 個區間(Low/Medium-Low/Medium/Good/Excellent),每區間挑**最接近中點**那天,畫出該天的 **processed 產品場**(同日多軌取聯集),疊區域邊界 + 海岸線。注意:畫的是覆蓋率產品本身,**不是**排放圖(通量散度排放不在本工具範圍)。

```python
from src.coverage import compute_coverage, plot_representative_days
df = compute_coverage("sentinel5p", "NO2___", "central", "2023-01-01", "2023-12-31")
plot_representative_days(df, output="rep_days.png")  # df 須單一 hub/product/region、daily
```

```bash
python -m src.coverage --hub sentinel5p --product NO2___ --region central \
    --start 2023-01-01 --end 2023-12-31 --granularity daily --rep-days rep_days.png
```

## QA 閾值掃描(raw L2,獨立模組)

`qa_sweep.py` 讀 **raw** TROPOMI L2,掃不同 `qa_value` 門檻對 bbox 內空間覆蓋率的影響——與 `compute_coverage`(讀 processed、QC 已套用)是不同層次,故獨立。region 僅支援 bbox。

```bash
python -m src.coverage.qa_sweep --product NO2___ \
    --start 2023-01-01 --end 2023-12-31 --qa 0.5 0.7 0.75 \
    --region figure --sample 500 --out qa_stats.csv --plot qa_sweep.png --report qa.md
```

輸出:per-granule CSV、2×2 圖(分布/箱型/均值±σ/均值-vs-qa)、Markdown 統計報告。`--sample` 帶 seed 可重現。

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

各 hub 原始/處理後檔結構見 [`SCHEMA.md`](SCHEMA.md)。

## 逐格統計(對時間 reduction → 地圖)

`maps.py` 與上面的覆蓋率計算**軸向正交**:`compute_coverage` 是對**空間** reduction
(區域內有效格/總格數)→ 時間序列;這裡是對**時間** reduction(每格有幾天有觀測)→ 地圖。

```python
from src.config.settings import LOCAL_WORK_DIR
from src.coverage import per_cell_stats, plot_cell_stats
st = per_cell_stats(LOCAL_WORK_DIR / "l3" / "MODIS_mcd19a2_aod_l3_02deg_2023.nc")
# st = {"mean", "count", "coverage"(0-100%), "n_time", "var"}
plot_cell_stats("...nc", product="MCD19A2", output="aod_cellstats.png")   # 三聯圖
```

⚠️ 這裡的 `count` 是**時間維度上有幾天有資料**,和 L3 檔裡的 `<var>_count`
(超取樣子點數 = 空間取樣密度)**不是同一件事**,同名不同義。

## Raw L2 footprint 圖

其餘功能都讀 processed 網格;`plot.py` 的這組讀 **raw L2**,把每個 sounding 畫成
它真正的 footprint 四邊形(角點直接取自檔案,非從中心推導)—— 這是唯一能看到
像元斜放、掃描邊緣脹大、軌道間真實空隙的視角,拿來目視檢查
`src.processing.l3` 的超取樣 binning 在 bin 什麼很直接。

```python
from src.coverage import plot_raw_l2_coverage
plot_raw_l2_coverage("2024-06-15", product="NO2___", region="central",
                     scale=6.022e23/1e4/1e15,      # mol/m2 → 1e15 molec/cm2
                     output="raw_l2.png")
```
