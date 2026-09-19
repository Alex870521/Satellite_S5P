# 統一 L3 Regrid Pipeline

三個衛星 source(S5P / GEMS / MODIS)共用**同一條純 Python 路徑**把 L2 swath 格網化成 L3,
取代原本三個各自重複的 processor。

```
detect_level(自動偵測檔案結構)
   ├─ L2 swath → Adapter(讀檔 → GranuleL2) → SupersampleBinRegridder(超取樣 binning)
   └─ L3 grid  → ingest_l3(讀 → 對齊同一 GridSpec;跳過 regrid)
   → L3Writer(CF nc: value + count)  /  L3Accumulator(時間聚合)
```

---

## 核心決策:超取樣 binning 取代 HARP

**問題**:L2 是逐軌 swath(斜放的大像元),要落到規則經緯網格。
- 散點插值(RBF)會 overshoot、平滑掉梯度,定量不可辯護。
- 點落格(`binned_statistic_2d` 中心落格)在原生解析度就掉 ~32% 覆蓋(像元中心不落格但
  footprint 蓋到該格 = oversampling 破洞)。
- Google Earth Engine / HARP 用 `bin_spatial` 面積加權,但 **HARP 不支援 GEMS/MODIS**,且是 native/conda 依賴。

**解法**:**footprint 超取樣 binning**(physical oversampling,Sun/Fioletov 法)
1. 從像元中心推 4 角點(`corners_from_centers`,誤差 0.1% 像元半徑,無損)。
2. 每像元 footprint 內 bilinear 灑 K×K(K=4)子點,權重 `qa/K²`。
3. 全部子點丟進 `binned_statistic_2d` 加權平均。
→ 大像元子點散落補滿覆蓋 + 子點落格比例 ≈ 面積佔比 → 面積加權自然浮現。

**驗證**(以 HARP 為 oracle;現在是 `tests/test_l3.py::test_vs_harp_oracle`):
- 5 軌 NO2(四季)+ 2 軌 O3 vs HARP:**r min 0.996 / mean 0.999**、bias ±0.04%、RMSE ≤4%。
- 覆蓋:`onlySS`(超取樣多出的格)永遠 = 0;`onlyH`(HARP 多出)平均 14.6 格(swath 最邊緣,K 提到 6 可收斂)。
- 角點推導 vs 真 `latitude_bounds`:0.1% 無損 → 沒 bounds 的 GEMS/MODIS 一樣能用。

**結論**:純 Python 數值上等價 HARP。**HARP 不進 pipeline,只當離線驗證 oracle**
(需要時手動裝、手動跑;報告引用「經 HARP/GEE 驗證 r=0.999」)。
⚠️ **2026-07-29 查證:本機 HARP env 已不存在**(`~/mamba/envs/` 整個沒了,micromamba 還在)
→ 相關測試現在都 skip;要重驗先 `micromamba create -n harp -c conda-forge harp`。

---

## 狀態

### ✅ Phase 1 vertical slice(S5P,已完成且綠)
- 套件 8 檔:`granule.py` / `regridder.py` / `writer.py` / `pipeline.py` / `adapters/{base,s5p}.py` + `__init__`。
- **未動** `src/api/*_api.py` 與舊 `SentinelProcessor/MODISProcessor/GEMSProcessor`(並行存在,import 無破壞)。
- 回歸(當時在 wip_l3,現已收進 `tests/test_l3.py`):S5P 一條龍 vs 自生 HARP oracle **r=0.9994**;`L3Accumulator` add×2 自一致 max|Δ|=2.7e-20。

### ✅ L2/L3 自動分流(已完成)
- `level.py` `detect_level(nc)`:看檔案結構自動判 L2/L3(1D 單調 lat/lon=L3;2D 或 scanline/ground_pixel 或 PRODUCT group=L2)。source-agnostic,不靠路徑。
- `ingest.py` `ingest_l3(nc, grid, product)`:已是 L3 就**跳過 regrid**,讀+對齊到同一 GridSpec(網格相同 → `l3_passthrough`;不同 → 線性內插 `l3_align`,等同裁到網格範圍),包成相同 GriddedField(value+count)。
- `L3Pipeline.process_file/build_field` 每檔先 `detect_level` 再分派;可傳 `level="L2"|"L3"` 手動覆寫。
- 驗證:真實 L2 granule→`supersample`(10476 格);合成全球 L3→`l3_align`;on-grid L3→`l3_passthrough`(值不變);pipeline 自動分派兩條都對。

### ✅ 三個 source 全接上(GEMS / MODIS adapter 已完成)
- `adapters/modis.py`:吃 `.nc`(`hdf4_to_netcdf` 的轉檔)或**直接吃 `.hdf`**。後者仍只透過
  `MODISProcessor` 的抽取方法碰 HDF4(沒有另開第二個 HDF4 入口),省下整年約 14GB 中繼檔;
  代價是需要 pyhdf(`[ingest]` extra)。MCD19A2 若為 (orbit,y,x) 會沿軌道 nanmean 收成 2D。
- `adapters/gems.py`:讀 `Data Fields`/`Geolocation Fields`,**不攤平**(超取樣需要 2D 才能推角點),
  把 QC 結果轉成 `qa` 權重交給 regridder 的門檻濾掉。AERAOD 三波長用 `band=` 選。
- `config/catalog.py` 補上 `MCD19A2`/`MOD04_L2`/`MYD04_L2`/`GEMS_NO2`/`GEMS_O3T` 的 ProductConfig,
  讓非 S5P 產品也有同一份 metadata 真相。

### ✅ 精確度數網格(統一解析度的前提)
`GridSpec` 原本只吃公里、再依緯度換算成度數 → 度數不是整數、格數會在 201/202 之間跳。
新增 **`GridSpec.from_degrees(deg, bounds)`** 直接用 `np.arange` 產生精確網格:
`from_degrees(0.02, (119,123,21,26))` → **251×201**,與模型 `config.TARGET_LAT/LON` 逐格 **max|Δ|=0**。
km 模式保留不動(回歸未破)。

### ✅ 階段二時間聚合(已接上)
- `L3Pipeline.aggregate(files, freq="D"|"M"|"Y")`:依時間分窗聚合,**串流**實作 —— 先用檔名日期
  排序、換窗即 finalize,任何時刻只有一個 accumulator 活著(~1.6MB;若先全讀再分組,整年 daily 約 350MB)。
  檔名順序與檔內時間不一致會 **raise**,不會靜靜輸出重複期別。
- `L3Writer.write_series(...)`:聚合結果 → 單一 `(time, lat, lon)` nc,含 `value`/`count`/`std`。
  座標直接用 **`lat`/`lon` 短名**、變數用短名(no2/o3/so2/aod)→ **模型端不必再 rename**
  (舊管線每次 merge 完都要手動改名的那一步,在這裡一次做對)。

### ✅ API 委派(附加,不動舊路徑)
`SatelliteHub.process_l3(product, start, end, out_path, deg=0.02, freq="D")` 走統一 pipeline;
各 hub 標 `L3_SOURCE`(s5p/modis/gems)。與既有 `process_data()`**並存**,呼叫端自選 ——
舊路徑是逐軌 RBF 內插到各產品原生網格,新路徑是超取樣 binning 到指定度數網格 + 時間聚合。
共用核心在 `runner.py::regrid_to_series()`,CLI 與 hub 都走它,不重複編排邏輯。

### ★ 新舊路徑實測比對(全年檔,共同遮罩)
| 產品 | 日均相關 | 日均偏差 | 逐格偏差 | 逐格 r | 覆蓋(舊→新) |
|---|---|---|---|---|---|
| NO₂ 2024(361 天) | **0.9995** | −0.00% | +0.00% | 0.9628 | 92.9% → 89.4% |
| O₃ 2023(365 天) | **1.0000** | +0.00% | +0.00% | 0.9983 | 96.3% → 94.4% |
| SO₂ 2024(321 天) | **0.9962** | −0.45% | −0.02% | 0.7527 | 71.0% → 56.4% |

**兩個容易誤讀的地方,都查清楚了:**

1. **逐格 r 對雜訊大的產品會偏低,不是錯誤。** SO₂ 的 `std/|mean| ≈ 1.4`(NO₂ 只有 0.85);
   RBF 平滑、binning 保留雜訊 → 逐格被雜訊拉低。把新場先做 5×5 平滑再比,r 回到 0.89。
   **在都有觀測的格上,兩者其實高度一致(日均 r=0.9962、偏差 −0.45%)。**

2. **「各自遮罩」的日均差會騙人。** SO₂ 用各自遮罩算是 +19.77%,用共同遮罩只有 −0.45% ——
   差別全在遮罩不同,不在數值。關鍵證據:在「舊有值、新無觀測」的那 **16.2%** 格上,
   舊值均值 = **−4.09e−05**,而共同格均值 = +2.13e−06。**SO₂ 柱量不該是強負值** →
   那些是 RBF 往無觀測區外插出來的非物理值,一直在把舊檔的平均值往下拉。
   → 新路徑對 SO₂ 不是比較差,是**明顯比較好**。

覆蓋率下降是**預期且正確**的:RBF 把沒觀測的格也內插填滿(所以恆接近 100%),binning 保留真實空洞。

### ⬜ 仍未做
- GEMS/MODIS 無 HARP oracle → 尚未做「點落格 vs 超取樣覆蓋差」的量化自驗。
- 舊 processor 尚未轉 deprecated shim(刻意:兩條路徑並存,還沒到淘汰時機)。

---

## 如何跑 / 驗證

驗證已從 gitignore 的 `wip_l3/` 收進版控。那些腳本各自複製了一份 production 邏輯
(`corners_from_centers`/`supersample` 現在都在 `regridder.py`),漂移風險高;現在拆成兩塊:

**可重用的功能 → `src/`**
- `harp_oracle.py`:`harp_oracle(raw, product, grid)` 跑 `harpconvert` 產獨立參考網格。
  **HARP 不在生產路徑上**,只是離線第二意見;沒裝就回 `None`,呼叫端 skip。
  ⚠️ `bin_spatial` 參數一律經 `GridSpec.harp_bin_spatial()` 產生(需 `.12g` 精度,否則 ~2e-5° 漂移)。
- `compare.py`:`compare_fields(a, b)` 標準比對指標。**兩個陷阱都內建處理**:
  偏差一律算在**共同遮罩**上(各自遮罩會給出 +19.77% 的假象);另外報 `only_a_mean`
  = 「A 有值、B 無觀測」那些格的均值,用來抓內插外插出的非物理值。

**測試 → `tests/test_l3.py`**
```bash
pytest tests/test_l3.py -m "not requires_data"   # 純邏輯,不需外接碟,CI 可跑(全套 32 項)
pytest tests/test_l3.py -m requires_data         # 三 source 讀真檔 + HARP oracle 對答案
```
需要外接碟/HARP 的會自動 skip 而非失敗。純邏輯層用合成資料驗:網格精確性、
**常數場經超取樣後必須仍是同一常數**(加權平均正確性)、qa 門檻、accumulator 等權重
==單純平均、`_period_key` 分窗、比對指標「A 獨有的格不可影響偏差」,
以及 **L2/L3 round-trip**(regrid→寫 L3 nc→`detect_level` 認出→`ingest_l3` 還原,max|Δ|=0)。

最小用法:
```python
from src.processing.l3 import GridSpec, SupersampleBinRegridder, L3Pipeline, S5PAdapter
grid = GridSpec(resolution=(5.5, 3.5))                       # NO2 原生;完整台灣 lattice
pipe = L3Pipeline(S5PAdapter("NO2___"), SupersampleBinRegridder(K=4), grid)
gf = pipe.process_file(raw_nc, out_nc="out.nc")             # → GriddedField(value, count)
```

---

## Gotchas / 注意
- **HARP = oracle only**,不是 pipeline 依賴;沒裝 HARP 也能跑完整 pipeline。
- **raw 路徑已重組(2026-06-09)**:`/Volumes/Transcend/Sentinel-5P/raw/L2/<species>/<year>/<month>/`
  (多一層 `L2/`;舊 `raw/NO2___/...` 已不在,`processed/` 也清空)。勿 hardcode 舊路徑。
- **HARP oracle 設定**:`brew install micromamba` → `micromamba create -n harp -c conda-forge harp`
  (這台無 conda;PyPI `harp` 是別的套件,別 pip 裝)。
- **HARP 輸出怪癖**(僅 oracle 用):網格編碼成 `latitude_bounds`/`longitude_bounds`(cell 邊界),不寫中心座標;
  `keep()` 不能列 latitude/longitude;`bin_spatial` 參數要 `.12g` 精度。
- **HCHO 已驗過(2026-07-29 更正)**:先前記為「qa≥0.5 台灣上空無資料 / 變數名待對」,兩者都不成立 ——
  變數名 `formaldehyde_tropospheric_vertical_column` 正確存在;台灣框內 qa≥0.5 有效點佔 97%;
  端到端聚合 30 檔 → 25 天全部有資料(逐日覆蓋中位 58.6%)。當初被 skip 的那 2 檔應是個別軌道沒掃到台灣。
  `tests/test_s5p_structure.py` 已把這件事釘住。
- **K(超取樣密度)**:K=4 已 r=0.999;swath 最邊緣覆蓋差想再收斂可調 K=6(成本 ∝ K²)。
- **`wip_l3/` 已刪除(2026-07-29)**:那些腳本各自複製了一份 production 網格化邏輯,
  且整批在 gitignore 外。功能收進 `harp_oracle.py`/`compare.py`,回歸改寫成 `tests/test_l3.py`。
