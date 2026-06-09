# 統一 L3 Regrid Pipeline

三個衛星 source(S5P / GEMS / MODIS)共用**同一條純 Python 路徑**把 L2 swath 格網化成 L3,
取代原本三個各自重複的 processor。

```
Adapter(讀檔 → GranuleL2)
   → SupersampleBinRegridder(footprint 超取樣 binning)
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

**驗證**(`wip_l3/validate_supersample.py`,以 HARP 為 oracle):
- 5 軌 NO2(四季)+ 2 軌 O3 vs HARP:**r min 0.996 / mean 0.999**、bias ±0.04%、RMSE ≤4%。
- 覆蓋:`onlySS`(超取樣多出的格)永遠 = 0;`onlyH`(HARP 多出)平均 14.6 格(swath 最邊緣,K 提到 6 可收斂)。
- 角點推導 vs 真 `latitude_bounds`:0.1% 無損 → 沒 bounds 的 GEMS/MODIS 一樣能用。

**結論**:純 Python 數值上等價 HARP。**HARP 不進 pipeline,只當離線驗證 oracle**
(裝在 `~/mamba/envs/harp/bin/harpconvert`,需要時手動跑;報告引用「經 HARP/GEE 驗證 r=0.999」)。

---

## 狀態

### ✅ Phase 1 vertical slice(S5P,已完成且綠)
- 套件 8 檔:`granule.py` / `regridder.py` / `writer.py` / `pipeline.py` / `adapters/{base,s5p}.py` + `__init__`。
- **未動** `src/api/*_api.py` 與舊 `SentinelProcessor/MODISProcessor/GEMSProcessor`(並行存在,import 無破壞)。
- 回歸 `wip_l3/regression_phase1.py`:S5P 一條龍 vs 自生 HARP oracle **r=0.9994**;`L3Accumulator` add×2 自一致 max|Δ|=2.7e-20。

### ⬜ 下一步(依序,需逐步 review)
1. **GEMS adapter** — 鏡像 `gems_processor.py` 讀 HDF5 `Data Fields`/`Geolocation Fields`,吐 GranuleL2(中心→推角點)。純新增檔。
2. **MODIS adapter** — 鏡像 `modis_processor.py` 的 pyhdf HDF4 讀法(需 `[ingest]` extra / py3.12-3.13)。
3. **三個 `*_api.py` 改委派** 新 L3Pipeline;舊 processor 轉 deprecated shim。公開介面(`fetch_data → process_all_files`)不變。
4. **Phase 2 聚合編排** — `L3Accumulator` 已實作,接 daily/monthly 分窗落檔(value/count/std)。
5. **收尾** — GEMS/MODIS 因無 HARP oracle,改用「點落格 vs 超取樣覆蓋差」自驗;HCHO 補驗(見下)。

---

## 如何跑 / 驗證

```bash
# 回歸(S5P 一條龍 vs HARP oracle,需先 /tmp/no2file.txt 指向一個 raw L2 NO2 檔)
.venv/bin/python wip_l3/regression_phase1.py

# 多軌多氣體驗證(超取樣 vs HARP)
.venv/bin/python wip_l3/validate_supersample.py
```

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
- **HCHO 尚未驗成功**:驗證掃描中 HCHO 2 檔被 skip(qa≥0.5 台灣上空無資料或 raw 變數名要再對),非阻塞。
- **K(超取樣密度)**:K=4 已 r=0.999;swath 最邊緣覆蓋差想再收斂可調 K=6(成本 ∝ K²)。
- `wip_l3/` 是 gitignored 的驗證腳本暫存區;正式碼在 `src/processing/l3/`。
