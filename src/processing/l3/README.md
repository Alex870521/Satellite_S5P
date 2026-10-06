# 統一 L3 Regrid Pipeline

S5P / GEMS / MODIS 共用一條純 Python 路徑,把 L2 swath 格網化到同一個度數網格(預設 0.02°,台灣 251×201),
再做時間聚合。

```
detect_level(看檔案結構自動判斷)
   ├─ L2 swath → Adapter(讀檔 → GranuleL2) → SupersampleBinRegridder(超取樣 binning)
   └─ L3 grid  → ingest_l3(對齊同一 GridSpec,跳過 regrid)
   → L3Accumulator(時間聚合) → L3Writer((time, lat, lon) nc:value / count / std)
```

## 用法

```bash
python -m scripts.l3_regrid_year --source s5p   --product NO2___        --year 2024
python -m scripts.l3_regrid_year --source s5p   --product NO2___        --year 2023 --freq granule
python -m scripts.l3_regrid_year --source gems  --product GEMS_NO2_TROP --year 2024
python -m scripts.l3_regrid_year --source modis --product MCD19A2       --year 2025 --dry-run
```

輸出到 `$LOCAL_WORK_DIR/l3/<SRC>_<產品>_l3_<度數>deg_<年>[_後綴].nc`,變數用短名(`no2` / `o3` / `aod` …)、
座標 `lat` / `lon` / `time`,模型端不必改名。Python 端:`SatelliteHub.process_l3(...)` 或
`runner.regrid_to_series(...)`(CLI 與 hub 共用)。

| 選項 | 預設 | 說明 |
|---|---|---|
| `--deg` / `--K` / `--qa` | 0.02 / 4 / 0.5 | 網格度數、超取樣密度、S5P `qa_value` 門檻 |
| `--freq` | `D` | `D`/`M`/`Y` 聚合;`granule` = 逐軌,每個 granule 一個時間步,時間為**過境時刻**,不合併同日多軌 |
| `--tz-offset` | GEMS 8,其他 0 | 分日用的時區(GEMS 依台灣當地日期) |
| `--cloud-max` / `--rms-max` | 0.3 / 0.005 | GEMS 雲量與 DOAS 擬合殘差上限(`none` 關閉) |
| `--slot-mode` / `--slots` | `per-slot` | GEMS 每個時槽一個年檔(檔名加 `_<HHMM>UTC`);`merge` 合成一個 |
| `--aod-band` / `--aod-qa` | `055` / `best` | MCD19A2 波段(470/550 nm)與 `AOD_QA` 篩選(雲遮罩 clear 且品質 0000) |

非逐日的預設檔名會加 `_granule` / `_M` / `_Y`,不會蓋到逐日年檔。設定值都寫進檔案 attrs。

## 方法:footprint 超取樣 binning

- RBF 散點內插會 overshoot、抹平梯度,還會往無觀測區外插出非物理值。
- 只用像元中心落格,在原生解析度就掉約 32% 覆蓋。
- HARP / GEE 的面積加權 `bin_spatial` 不支援 GEMS / MODIS,而且是 native 依賴。

做法(Sun / Fioletov physical oversampling):由像元中心推 4 個角點(誤差約 0.1% 像元半徑)→ 每個 footprint
內灑 K×K 個子點、權重 `qa/K²` → 子點落格加權平均。子點落格比例近似面積佔比,面積加權自然成立。
逐日聚合以 `count` 為權重;串流處理,記憶體只留一個 accumulator。

## 驗證

**對 HARP**(離線 oracle,`tests/test_l3.py::test_vs_harp_oracle`,六種 S5P 產品 × 3 個月):

| 產品 | r | 偏差 |
|---|---|---|
| NO₂ | 0.996–0.998 | ≤ 0.12% |
| O₃ | 0.996–0.9997 | ≈ 0 |
| CO | 0.991–0.998 | ≤ 0.03% |
| CH₄(東亞框) | 0.9986–0.9995 | ≈ 0 |
| SO₂ | 0.986–0.991 | ≤ 0.82% |
| HCHO | 0.975–0.986 | ≤ 0.23% |

SO₂ / HCHO 訊號弱、雜訊大,r 隨 K 增加而上升(HCHO K=16 → 0.991),是取樣精細度而非方法差異。
AER_AI 碟上無原始檔,未驗。HARP 不在生產路徑上:`micromamba create -n harp -c conda-forge harp`
(PyPI 的 `harp` 是別的套件),沒裝時相關測試 skip。

**對舊 RBF 年檔**(共同遮罩):日均 r NO₂ 0.9995、O₃ 1.0000、SO₂ 0.9962,偏差 ≤ 0.45%。
覆蓋率較低(NO₂ 93% → 89%、SO₂ 71% → 56%)是預期的:RBF 會把無觀測格也填滿,而 SO₂ 舊檔在那些格上
是外插出的強負值。

> [!IMPORTANT]
> 新舊比對務必用**共同遮罩**(`compare.compare_fields` 已內建);各自遮罩會給出約 +20% 的假偏差。

## 各來源注意

- **S5P**:檔案的 `time` 只是當天 00:00 參考時間;granule 時間取「落在網格內的掃描線」平均 `delta_time`
  (台灣約 04–06 UTC)。raw 佈局為 `Sentinel-5P/raw/L2/<產品>/<年>/<月>/`。
- **GEMS**:`ColumnAmountNO2` 是總柱量,和 TROPOMI 比要用 `GEMS_NO2_TROP`。adapter 不攤平(要 2D 推角點),
  QC 轉成 `qa` 權重;AERAOD 用 `band=` 選波長。
- **MODIS**:`.hdf` 經 `MODISProcessor` 的抽取方法讀(需 `[ingest]` 的 pyhdf)。MCD19A2 沿軌道 nanmean 收成逐日;470 nm 約比 550 nm 高 17%,QA best 冬季約只剩一半格點。
  `--aod-band 047 --aod-qa none` 可逐格重現舊口徑。MOD04/MYD04 讀 `AOD_550_Dark_Target_Deep_Blue_Combined`。

> [!WARNING]
> MCD19A2 是多軌已合併的逐日 tile,沒有逐軌時間,不能用 `--freq granule`(會因同時刻而報錯)。

## 與舊處理器的關係

各 hub 的 `process_data()`(`SentinelProcessor` / `GEMSProcessor` / `MODISProcessor`)是較早的逐檔流程:
RBF 內插到各產品原生網格,一個 granule 一個檔。它仍然保留,適合要「一軌一檔、原生解析度」的用途;
需要跨衛星、跨年比對或模型輸入時,建議改用本 pipeline(`process_l3()` / `scripts/l3_regrid_year.py`)。

| 需求 | 建議 |
|---|---|
| 跨衛星 / 跨年比對、模型輸入(統一網格) | 本 pipeline,`--freq D` |
| 單次過境分析(例如配合當下風場) | 本 pipeline,`--freq granule` |
| 逐檔出圖、逐站 CSV、GeoTIFF | 舊處理器(L3 尚無對應輸出) |

## 測試

```bash
pytest tests/test_l3.py tests/test_l3_granule.py tests/test_modis_aod_options.py -m "not requires_data"   # 純邏輯,CI 跑
pytest tests/test_l3.py -m requires_data   # 讀外接碟真檔 + HARP oracle,缺資料時 skip
```

涵蓋網格精確性、常數場經超取樣仍為同一常數、qa 門檻、聚合權重、分窗與逐軌時間、L2→L3→L2 round-trip
(max|Δ|=0)、比對指標不受單邊獨有格影響。

最小 API:

```python
from src.processing.l3 import GridSpec, SupersampleBinRegridder, L3Pipeline, S5PAdapter
grid = GridSpec.from_degrees(0.02, (119, 123, 21, 26))
pipe = L3Pipeline(S5PAdapter("NO2___"), SupersampleBinRegridder(K=4), grid)
series = pipe.aggregate(raw_files, freq="D")          # [(period, {"value","count","std"}), …]
```
