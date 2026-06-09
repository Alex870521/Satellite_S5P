# 衛星資料 Schema 參考(原始檔 vs 處理後檔)

各 hub 的檔案結構紀錄,供日後更新 reader / processor 或新增 hub 時對照。
分兩層:**raw（下載的原始檔，未裁切/未網格化）** 與 **processed（`process_data` 寫出、本工具實際讀的檔）**。
驗證時間 2026-06,對照磁碟 `/Volumes/Transcend`(= `BASE_DIR`)。

> 本工具(`src/coverage`)只讀 **processed**。raw 欄位列出來是為了知道 processor 從哪來、未來改格網時怎麼動。

---

## 1. Sentinel-5P  (`SENTINEL5PHub`, `src/api/sentinel_api.py`)

> **2026-06 結構變更**:Sentinel-5P 的 `raw / processed / figure / geotiff` 都多一層**處理階層**:
> `<kind>/<LEVEL>/<PRODUCT>/...`。`LEVEL` = `L2`(Copernicus 自下載內插)或 `L3`(S5P-PAL 官方
> 全球網格裁切,見 `run_l3_pipeline`)。coverage `HubSpec.level='L2'`。只有 Sentinel 有這層;
> GEMS/MODIS/ERA5 維持 `<kind>/<PRODUCT>/...`。

### raw — L2 swath（Copernicus 原始,未動）
- 路徑:`Sentinel-5P/raw/L2/<PRODUCT>/<YYYY>/<MM>/S5P_*_L2__<PRODUCT>_*.nc`
- **有 group**:資料在 `group='PRODUCT'`(讀取要指定)。
- 維度:`scanline`(~4172) × `ground_pixel`(450) × `time`(1) + 輔助維(corner=4, layer=34…)。
- 座標:`latitude` / `longitude` 為 **2D (scanline, ground_pixel)** 的 swath 經緯。
- 關鍵變數:`nitrogendioxide_tropospheric_column`(mol/m²)、`qa_value`(0–1)、`*_precision`、`averaging_kernel`…
- 像素角點:`group='PRODUCT/SUPPORT_DATA/GEOLOCATIONS'` 的 `latitude_bounds` / `longitude_bounds` (…,4)。
- QC:用 `qa_value >= 0.75`(emission 線)/ `>=0.5`(研究預設,見記憶)。

### processed — 規則網格（本工具讀這個）
- 路徑:`Sentinel-5P/processed/L2/<PRODUCT>/<YYYY>/<MM>/<原檔名>.nc`(沿用原檔名)
  - L3(S5P-PAL 裁台灣):`Sentinel-5P/processed/L3/<product-id>/<agg>/<item>.nc`(全球網格裁切,product-id 如 `no2-tropospheric`)。
- **無 group**(攤平),`xr.open_dataset` 直接讀。
- 維度:`(time=1, latitude, longitude)` — **單檔單時刻**(一軌一檔;同日可多軌)。
- 座標:`latitude` / `longitude` 皆 **1D**(固定 GridFrame,同產品每檔同網格)。`time` = 該日 00:00。
- 變數:單一,依產品(見 `registry._S5P_VARS`),如 NO2 = `nitrogendioxide_tropospheric_column`。
- 解析度:依產品(NO2 台灣框 ≈ 159×74)。NaN = 已被 QC/雲篩掉。
- 產品代碼:`NO2___ O3____ SO2___ HCHO__ CH4___ CO____ AER_AI`(碟上現有 CH4/HCHO/NO2/O3)。

---

## 2. Sentinel-3  (`SENTINEL3Hub`, 與 S5P 共用 `SentinelHubBase`)

- **processed schema 與 Sentinel-5P 相同**(同 base class):`(time, latitude, longitude)`、1D 座標、無 group。
- 產品代碼不同(OLCI / SLSTR 類)。
- raw 為 OLCI/SLSTR L1/L2(`file_class` 用 `NT`/`NR`,非 `NTC`)。
- **狀態**:碟上 `Sentinel-3/processed` 目前無資料(尚未歸檔);reader 已就緒,變數以「第一個 data_var」自動解析。

---

## 3. GEMS  (`GEMSHub`, `src/api/gems_api.py`)

### raw — L2（NIER/NESC,HDF5）
- 路徑:`GEMS/raw/<PRODUCT>/<YYYY>/<MM>/GK2_GEMS_L2_*.nc`
- **有 group**:`Data Fields` + `Geolocation Fields`。
- 主變數:`ColumnAmountNO2` / `ColumnAmountO3` / `ColumnAmountHCHO` / `ColumnAmountSO2` / `FinalAerosolOpticalDepth`(多波長)。
- 像素 ~8 km(EW) × 3.5 km(NS),靜止軌每日多時刻。

### processed — 規則網格
- 路徑:`GEMS/processed/<PRODUCT>/<YYYY>/<MM>/<原檔名>.nc`
- 維度:`(time=1, latitude, longitude)`、1D 座標、無 group(攤平)。`time` 由檔名解析(如 0345 UTC)。
- AERAOD 特例:輸出 `FinalAerosolOpticalDepth_{354|443|550}nm` 多變數 → reader 用「第一個 data_var」回退(`registry._GEMS_VARS['AERAOD']=None`)。
- 實測檔:`(time=1, latitude=159, longitude=51)`、var `ColumnAmountNO2`、time 含**時刻**(靜止軌一天多筆,如 00:45 UTC)。
- **資料位置(不在預設 Transcend)**:逐檔版 `/Users/chanchihyu/DataCenter/Satellite/GEMS/processed/<PRODUCT>/<YYYY>/<MM>/`;另有整年合併檔 `/Volumes/TOSHIBA/GEMS/processed/GEMS_NO2_merged_20220101_20231231.nc`(flat)。
- **狀態**:reader **已驗證可用**,用 `--base-dir /Users/chanchihyu/DataCenter/Satellite`(或 TOSHIBA)指向即可。⚠️ 多時刻需注意 end 日期會自動補到當日 23:59:59。

---

## 4. MODIS  (`MODISHub`, `src/api/modis_api.py`)

### raw — swath（HDF4）
- 路徑:`MODIS/raw/<PRODUCT>/<YYYY>/<MM>/<PRODUCT>.A<YYYYDDD>.<HHMM>.061.*.hdf`(如 `MYD04_L2.A2023001.0505...hdf`)
- HDF4 SDS;`Latitude`/`Longitude` 為 **2D swath**;主資料 = AOD(`Optical_Depth_Land_And_Ocean` 類)。
- 產品:`MOD04_L2`(Terra)/`MYD04_L2`(Aqua)/`MCD19A2`(MAIAC)。

### processed — **已網格化整年單檔**（與推測不同,重要）
- 路徑:`MODIS/processed/<PRODUCT>/<PRODUCT>_<YYYYMMDD>_<YYYYMMDD>.nc`(**flat,非年/月夾**)
- 維度:`(time=N天, lat, lon)` — 實測 2023 = `(364, 52, 41)`。
- 座標:`lat` / `lon` **1D 規則 0.1° 網格**(已從 swath 重投影);`time` 逐日。
- 變數:`aod`。→ 對本工具而言與 S5P 同樣是「規則 (time,lat,lon) cube」,只是座標名 `lat/lon`、時間維 >1、檔案 flat。
- registry 對應:`layout="flat"`、`lat_names=("lat",...)`。

---

## 5. ERA5  (`ERA5Hub`, `src/api/era5_api.py`) — 與濃度覆蓋率語意不同

### raw — 再分析場（CDS,nc）
- 路徑:`ERA5/raw/single_level/era5_*_<YYYY>.nc`、`ERA5/raw/pressure_level/...`
- 維度:`(time 逐時, latitude, longitude[, level])`、1D 座標。
- 變數:`u10/v10/t2m`、PBL、`temperature/relative_humidity/u/v`(壓力層)…多變數氣象場。

### processed — **逐站 CSV，非 nc**
- 路徑:`ERA5/processed/csv/<YYYY>/era5_*_station_*.csv`(**碟上 `processed/csv` 目前為空,尚未產出**)
- 內容:逐時 × 站點(可含 3×3 grid 9 點 + mean/std)。
- **覆蓋率語意**:再分析無雲遮,空間覆蓋恆 100%。此處「覆蓋率」應改指 **時間可用率**(實到小時數 / 應有小時數)。
- reader 狀態:**stub**(`ERA5Reader`,warn + 空);待補時序可用率邏輯。

---

## 6. Himawari  (`HimawariHub`, `src/api/himawari_api.py`)

- **目前整支是 mock**:authentication / fetch / download 皆模擬,`process_data` 未實作,無 processed 檔。
- 預期(實作後):產品 VIS/IR/WV/BAND03–16,10min–日 多時距;processed 推測 `(time, y, x)` 或重投影 `(time, lat, lon)`。
- reader 狀態:`MockReader`,warn + 不產 slice。等真實 processor 上線再依實際 schema 補 `HubSpec`。

---

## 統一抽象(本工具如何吃這些差異)

| Hub | reader | layout | 座標名 | time | 變數解析 | 現有資料 |
|---|---|---|---|---|---|---|
| Sentinel-5P | GriddedNCReader | year_month | latitude/longitude | 單檔1筆 | 依產品表 | ✅ CH4/HCHO/NO2/O3 |
| Sentinel-3 | GriddedNCReader | year_month | latitude/longitude | 單檔1筆 | 第一個 var | ⬜ 未歸檔 |
| GEMS | GriddedNCReader | year_month | latitude/longitude | 單檔1筆 | 依產品表/回退 | ⬜ 不在此碟 |
| MODIS | GriddedNCReader | flat | lat/lon | 多筆(逐日) | aod | ✅ MYD04_L2 |
| ERA5 | ERA5Reader(stub) | flat(csv) | — | 逐時 | — | csv(待接) |
| Himawari | MockReader | — | — | — | — | ⛔ mock |

**新增 hub / 改格網的最小改動**:多數情況只要在 `registry.HUB_SPECS` 加一條 `HubSpec`(路徑夾名、layout、座標名候選、產品→變數對應)。只有「2D swath 未網格化」或「非 nc」才需要寫專屬 reader。
