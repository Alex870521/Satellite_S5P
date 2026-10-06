# Satellite_S5P 結構審視與優化計畫(2026-09-09)

> 狀態:**草案,未 commit**。基準 `main @ e20aec7`(領先 origin 16 個 commit)。
> **2026-09-10 進度:Phase 3(C12/C11/C9)全部完成並 commit(3805b74/08895bd/4cde537);附帶抓到 B24(S5P 檔名 regex 抓到結束時間)並修。**
> **2026-09-09 進度:Phase 0 + Phase 1 全部 10 項已完成並 commit(9ca01d1 / 34bf4be / 78daa29);同環境測試 39 passed / 31 skipped / 0 fail(Transcend 已拔,基準隨之調整)。**
> 由一次唯讀審視產生(codebase-health agent 深度掃描 + 人工抽驗),**沒有改任何程式**。
> 三個問題需要 repo 擁有者先拍板(§4),其餘可依 §5 的順序執行。

---

## 0. 一頁摘要

**整體**:程式能跑、測試能過(44 passed / 0 fail)、L3 新路徑品質好且有測試。問題集中在三類:
**守門失效**(CI 從不跑測試)、**兩套真相**(產品設定、變數名、依賴檔各自兩份)、**測試偏斜**(43/44 在 L3,其餘 8,000 行零測試)。

**最該先做的一件事**:`.github/workflows/pytest.yml` 在 `pip install` 之後就結束,**沒有 pytest step**;
而且它裝的 `.[test]` extra 根本不存在。每次 push 都綠燈、實際跑 0 個測試。其他所有改動都該等這道護欄上線。

**三個待拍板**(§4):AER_AI 用哪個波段對;GEMS 產品清單以哪份為準;CI 要擋到什麼程度。

---

## 1. 功能地圖

| 區塊 | 規模 | 狀態 | 對外入口 | 主要消費者 |
|---|---|---|---|---|
| `src/api/` 六個 hub | 3,485 行 | 生產 | `SatelliteHub.run_pipeline` / `process_data` / `process_l3` | `automation/run_pipeline.py`、`examples/` |
| `src/processing/` 舊 processor(RBF) | 3,588 行 | **過渡,活的** | 三個 hub 的 `process_data()` 內 lazy 建構 | `run_pipeline.py:184,223,303`、`examples/ESA_Sentinel3.py` |
| `src/processing/l3/` 統一 L3 pipeline | 1,011 行 | 生產,**唯一有測試** | `runner.regrid_to_series`、`scripts/l3_regrid_year.py` | `wip_gems_tropomi/` 五支核心腳本 |
| `src/coverage/` | 1,512 行 | 生產,零測試 | CLI `python -m src.coverage`、`compute_coverage` | `wip_emission/s5p/plot_emission_representative_days.py`(只用 `load_raw_l2`/`plot_raw_l2_pixels`) |
| `src/merge/` | 213 行 | 生產,單一用途 | `python -m src.merge` | `scripts/fetch_merge_delete_raw.py` |
| `src/visualization/` | 1,141 行 | 生產 | **`plot_nc.basic_map` 是全 repo 最重的下游介面**:14 個 wip 檔約 30 個呼叫點 | wip_* |
| `src/config/` | 1,218 行 | 生產 | `catalog.PRODUCT_CONFIGS`(權威表)、`settings.BASE_DIR` | 全部 |
| `src/utils/` | 331 行 | 生產 | `extract_datetime_from_filename`(L3 聚合排序的隱含前提) | `core.py`、`l3/pipeline.py` |
| `automation/run_pipeline.py` | 456 行 | 排程入口(`schedule`) | — | launchd / 手動 |
| `scripts/` 7 支 | 616 行 | 生產 | — | 手動;`l3_regrid_year.py` 是 L3 年檔的實際 CLI |
| `tests/` | 70 個 | 44 passed / 26 skipped(全 `requires_data`) | — | — |

**休眠 / 疑似死碼**(已含 7 個 wip 目錄掃描):
- `HimawariHub`:mock,`processor` 永遠回 None(`himawari_api.py:286-306`),`__init__` 外零消費者。
- `RbfRegridder`:`l3/__init__` 匯出但全 repo 零呼叫。
- `plot_taiwan.py` 與 `plot_taiwan_power_plant.py` 各有一個**同名同簽名**的 `plot_taiwan_map`,皆零外部消費者,行為不同(§2 B9)。
- `config/richer.rich_print`、`modis_processor.{process_hdf_file,_merge_all_files,_merge_tile_data,_merge_daily_tiles,_direct_fill_interpolation}`、`sentinel_processor.debug_dataset_structure`:零呼叫者。
- **不算死碡、是刻意停用**:`process_files_to_csv`、`save_as_tiff`(呼叫點已註解並留有說明),不動。

**MODIS 現在有三條網格化路徑**:舊 RBF processor、`modis_daily_grid.py`(drop-in-the-box,`modis_api` 與 `coverage/registry` 在用)、L3 supersample。

---

## 2. 健康度發現

### P0

| # | 位置 | 問題 |
|---|---|---|
| **B1** | `.github/workflows/pytest.yml:34` | 安裝完就結束,**沒有任何執行 pytest 的 step**;`.[test]` extra 不存在(只有 `ingest`/`dev`),pip 對未知 extra 只警告。CI 實際測試數 = 0 |
| **B2** | `catalog.py:279` vs `coverage/registry.py:60` | AER_AI 變數名兩套:processor 寫檔用 `aerosol_index_340_380`,coverage/merge 找 `aerosol_index_354_388`。兩者在 TROPOMI L2 都真實存在,是不同波段對 → 不是打錯字,是兩處各自做了科學選擇後失去同步 |
| **B3** | `catalog.py:10-13` vs `:205-353` | `CLOUD_`/`FRESCO`/`AER_LH` 在 `PRODUCT_TYPES` 卻不在 `PRODUCT_CONFIGS`:可下載、處理時 `sentinel_processor.py:813` KeyError。`README.md:145,245` 還把 `CLOUD_` 列為可用 |
| **B4** | `plot_nc.py:191` | `Dataset(dataset,'r').groups` 開了不關;所在的 `plot_global_var` 在 6 處逐檔迴圈端被呼叫,整年批次會累積 fd 到撞 ulimit |

### P1

- **B5 runner「單一編排入口」契約失效**。`runner.py:3-4` 說 CLI 與 `process_l3` 都走 `regrid_to_series`;實際 `l3_regrid_year.py:119-153` 完整重寫編排、從未呼叫它。`_make_adapter`(`:65-75`)與 `runner.make_adapter`(`:33-45`)逐行相同;bounds 常數兩份。
- **B6 GEMS 產品設定兩套且值不同**。`gems_processor.py:45-58` 六項、友善名 key、O3T `200/400`;`catalog.py:317-352` 四項、adapter key、O3T `None/None`。新 adapter 走 catalog,舊 processor 走自己那份。
- **B7** `RbfRegridder` qa 門檻寫死 0.5(`regridder.py:152`),`__init__` 無參數;同檔 `SupersampleBinRegridder` 可配置。
- **B8 掃檔 + 日期過濾 + 按月分組共六份**。`sentinel_processor.py:863-924` 與 `modis_processor.py:508-573` 66 行僅差 12 行;另四份在 `modis_processor.py:649-706`、`sentinel_processor.py:431-469`、`gems_processor.py:407-428,459-479`。第七份 `l3_regrid_year.py:41-62` 繞過 `BASE_DIR` 另起碟片探索。
- **B9** `plot_taiwan_map` 兩份:預設 extent `[119,123,21,26]` vs `[120,121.5,23.4,25]`,shapefile fallback 不同,簽名相同 IDE 不會提示。
- **B10 靜默吞例外**。`sentinel_processor.py:461-462` 裸 `except: continue`;`regridder.py:68-71` 角點推導失敗回全 NaN 場無 log(與「當天無觀測」不可區分);`core.py:204-206,253-256` 兩個裸 except 時區失敗改用 UTC。新路徑大量無訊息 `return None`(`adapters/gems.py:71,75,84,103,107`、`adapters/modis.py:56,61,65,89,103,116,125`、`adapters/s5p.py:33`)。
  ⚠️ **修正 agent 原描述**:`pipeline.py:137-138` 檔名日期解析失敗會退回字典序,但 `pipeline.py:106-111` 的重複期別偵測**會 raise**(不是靜靜失效);真正的風險是「解析失敗的檔被排到尾端,可能觸發那個 raise 中斷整批」。
- **B11** `README.md:65` 的 `CDSAPI_URL` 仍是退役的 `/api/v2`;`.env.example:12-13` 已註明應改 `/api`。
- **B12** README 佈局圖兩處錯:`:335` S5P 樹缺 `L2/` 層;`:336` MODIS processed 寫成年月夾,實際 flat(`registry.py:85-86`、`SCHEMA.md:74`)。
- **B13** docs 死連結:`docs/MODIS_HDF_Merge_README.md:128` 指向不存在的 `examples/merge_modis_hdf_example.py`;`docs/MODIS_AOD_Variables_README.md:59-76` 要人 cd 進已於 55c4c5d 移除的 `merge_data/MODIS`。
- **B22(新)打包會壞**。`pyproject.toml` `[tool.setuptools] packages = ["src"]` 只含頂層;非 editable 的 `pip install .` 會漏掉 `src.api / config / coverage / merge / processing / processing.l3 / processing.l3.adapters / visualization` 全部 8 個子套件。**README Installation 教的正是 `pip install .`**。目前本機沒踩到是因為以 repo 根 + `sys.path` 執行。

### P2

- **B14 依賴管理兩套且矛盾**。pyproject `>=`、requirements `~=`。`requirements.txt:12` 列了 PyPI 的 **`logging~=0.4.9.6`**(stdlib 同名,PyPI 那個是 2005 年廢棄包,裝到會遮蔽 stdlib)。宣告但零 import:dask、schedule、tqdm、rioxarray、tkcalendar、urllib3(rioxarray 例外:支撐刻意停用的 GeoTIFF 分支 `sentinel_processor.py:994-1013`)。
- **B15** black / isort / mypy 在 pyproject 設定完整,但 venv 全部未安裝、CI 零呼叫 → 純裝飾。
- **B16 硬編路径**。`scripts/` 五處無 env 覆寫:`l3_regrid_year.py:35,95`、`fetch_merge_delete_raw.py:29`、`download_process_aod.py:27,57`、`download_epa_stations.py:38,42`。`src/` 內只有 `settings.py:29` 一處且可用 `SATELLITE_BASE_DIR` 覆寫(健康)。`download_epa_stations.py:100` 跨 repo import aero-web-server 的 `backend.utils.tw_gov_tls`。
- **B17 效能**。`interpolators.py:69-72` 每個觀測點對整張網格算距離矩陣(1e5 × 5e4,且是 griddata 預設分支);`gems_processor.py:484-494` 整年 list 再 stack;`pipeline.py:40,44` 每檔開 2–4 次。
- **B18** 零呼叫者的舊碼(見 §1 休眠清單)。
- **B19 重複 helper**。`_pick` 在 `l3/ingest.py:25-29` 與 `coverage/reader.py:30-33`(差一行隱含 return);GEMS 時間 regex 三份;台灣 bbox 七份;公里↔度數常數兩份(`grid_frame.py:18,22`、`l3/granule.py:37-38`)。
- **B20 測試缺口**。零測試:五個 hub、`src/merge`、`src/coverage`、`src/visualization`、三個舊 processor、`extract_datetime_from_filename`(L3 聚合正確性的隱含前提)。`test_basic.py` 只驗 import。
- **B21 Python 版本三套說法**。CI 只有 3.12;`tox.ini` 宣告 312/313/314(從未在 CI 跑);venv 是 3.13;mypy/black target 釘 312。
- **B23(新)pyproject metadata 是佔位**。`name = "s5p-processor"`(早已多衛星)、`authors = Your Name <your.email@example.com>`。

### 基準狀態(審視當下)

| 檢查 | 結果 |
|---|---|
| 工作區 | 乾淨 |
| 模組 import(11 個) | 全部成功 |
| pytest | 44 passed / 26 skipped / 0 fail(skip 全為 `requires_data`,Transcend 未接) |
| `-m "not requires_data"` | 收 32/70(這是 CI 修好後會跑的數量) |
| adapter Protocol | 三個 adapter 全符合 `L2Adapter` |
| `pip check` | No broken requirements |
| CI 實際跑的測試數 | **0** |
| lint / typecheck | 未安裝、未執行 |

---

## 3. 優化計畫

每項標:動哪些檔、誰受影響、怎麼驗證。**🔒 = 動到 CI 或會改變日常呼叫方式,需先點頭。**

### 低風險,立刻可做

| # | 做什麼 | 動哪 | 驗證 |
|---|---|---|---|
| **C1** 🔒 ✅ | CI 補 `pytest -m "not requires_data"`;`.[test]` → `.[dev]` | `pytest.yml:34` 後加一步 | 本機無碟已跑過:32 收、全過。**其他一切的護欄,排第一** |
| **C7** ✅ | 刪 `requirements.txt:12` 的 `logging` | 一行 | 乾淨 venv 裝一次,`logging.__file__` 指 stdlib |
| **C19(新)** ✅ | 修打包:`packages = ["src"]` → `[tool.setuptools.packages.find] where=["."] include=["src*"]` | `pyproject.toml` | `pip install .` 到乾淨 venv,`python -c "import src.processing.l3"` |
| **C2** ✅ | 修 `plot_nc.py:191` fd 洩漏(with 區塊或改 xarray 探測 group) | 一行 | 多檔批次時 `lsof -p <pid> \| wc -l` 不隨檔數成長。`basic_map` 輸出不變,wip 不受影響 |
| **C4** ✅ | `CLOUD_`/`FRESCO`/`AER_LH` 早點失敗:`sentinel_processor.py:813` 取 config 前檢查 key,給「可下載但尚未設定處理參數」訊息;README `:145,245` 標「處理未支援」 | 兩檔 | 行為不變,KeyError 換成有意義訊息 |
| **C5** ✅ | README 三處事實錯誤(`:65` → `/api`;`:335` 補 `L2/`;`:336` 改 flat)+ Documentation 區補 `l3/README.md`、`coverage/README.md`、`merge/README.md` 三個連結(三份都存在但主 README 沒指過去) | `README.md` | 目檢 |
| **C6** ✅ | docs 兩份 MODIS 文件的失效段落加「已於 55c4c5d 移除,現行入口 `modis_processor.merge_hdf_files_to_netcdf`」 | 兩個 md | 目檢 |
| **C8** ✅ | `l3/README.md:125` 測試數 21 → 實際數(全套純邏輯 32) | 一行 | — |
| **C20(新)** ✅ | pyproject `name`/`authors` 填真值 | `pyproject.toml` | — |
| **C13** ✅ | `RbfRegridder` 補 `qa_threshold` 參數,預設 0.5 不變 | `regridder.py:140-152` 兩行 | 零呼叫者,零風險;趁沒人用時把介面修對 |

### 中風險,需要測試護欄(每項都是「先寫測試、再改」)

- **C3 ✅ 統一 AER_AI 變數名** —— D1 拍板:**兩個波段對都保留**。決定後只改 `catalog.py:279` 或 `registry.py:60` 一處。改 catalog → 既有已處理檔變數名與新檔不一致,要一併決定重跑;改 registry → 既有檔立刻能被 coverage 讀到。驗證:對一個既有 AER_AI processed 檔跑 `python -m src.coverage --hub sentinel5p --product AER_AI`。
- **C10 ✅ GEMS 產品設定收斂到 catalog** —— D2 拍板:**六項全保留、O3T 200/400 保留**。`gems_processor.py:45-58` 改查 `PRODUCT_CONFIGS`;processor 獨有的科學設定(O3T 200/400、HCHO/SO2/AERAOD/UVI 四項)搬進 catalog。`wip_gems_tropomi/` 走 l3 adapter(已用 catalog)不受影響;舊 processor 圖色階會變。驗證:**改之前**先把兩份設定差異釘成 `test_gems_config_matches_catalog`,收斂後每個欄位都是刻意值。
- **C12 ✅ 抽掉 `_pick` 與 GEMS regex 的重複**。`_pick` 抽到 `src/utils/nc_names.py`;三份 GEMS regex 收斂成呼叫 `extract_datetime_from_filename`。`l3/adapters/gems.py` 是 wip 重度依賴,但 `read()` 簽名與行為不變。驗證:**先**為 `extract_datetime_from_filename` 補測試(S5P/MODIS/GEMS 三種檔名)—— 它零測試卻是 L3 聚合的隱含前提。
- **C11 ✅ 靜默失敗變有訊息**。`sentinel_processor.py:461` 裸 except 具名 + log;`pipeline.py:137` 排序退化發 warning;`regridder.py:68-71` 全 NaN 場加 debug log;`core.py:204,253` 具名。四檔各約三行,l3 行為不變。驗證:餵一個檔名日期壞掉的檔進 `aggregate`,要看到 warning。
- **C9 ✅ 讓 `l3_regrid_year.py` 真的走 `regrid_to_series`**(B5 收斂)。`main` 改呼叫 runner、刪 `_make_adapter` 改用 `runner.make_adapter`、`BOUNDS` 改 `runner.DEFAULT_BOUNDS`;`_discover` 保留(`tests/test_l3.py` import 它)。驗證:**先**加測試對同一批 fixture 分別走 CLI 與 `regrid_to_series`,斷言輸出 nc 的變數/attrs/數值完全相同,通過後再重構。
- **C14 ✅(第 1 步、第 2 步皆 2026-10-06 完成)** 安裝並執行 lint/mypy。**實測**:整個 `src/` 375 個錯(約 7 成在三個舊 processor),只看 l3 21 個、coverage 13 個 —— 全部是型別標註問題,沒有執行期 bug。**第 1 步已做**:l3 + coverage 歸零(`cast`/標註/`assert`/等價 API,零行為變更);`catalog` 的 `Literal[變數]` 改成先寫 Literal 再 `get_args()`(執行期常數逐值相同);pyproject 設 `files = [l3, coverage]`、舊 processor 等模組 `ignore_errors`、無型別檔的第三方套件 `ignore_missing_imports` → **repo 根目錄直接 `mypy` 即 Success**。**第 2 步已做**:CI 加 `Type check (mypy)` 一步、會擋。推之前用 uv 重建 CI 同款 3.12 乾淨環境實跑,抓到兩個「本機過、CI 會紅」的差異:① 本機 venv 剛好裝了 pandas-stubs/types-pytz,CI 沒有 → 加進 `dev` extra;② CI 依賴未鎖版本、會裝到 numpy 2.5/pandas 3.0(本機 2.1/2.2),新版型別更細 → 3 處改成兩版都過的寫法。兩環境 mypy 皆 Success、測試皆全過。原計畫:先只對 `src/processing/l3/` 與 `src/coverage/` 跑 mypy,CI 加一步 `continue-on-error`,看噪音量再決定要不要擋。

### 大重構,要先討論

- **C15 ✅ 舊/新路徑的收斂條件(產出是文件,不是重構)**。→ 2026-10-06 寫進 `src/processing/l3/README.md`「舊路徑的退場條件」,附四條的現況與驗證指令:**目前全未滿足**(wip 有 36 個腳本讀舊佈局)。2026-10-06 補第 5 條:**aero-web 網站衛星頁直接讀舊路徑的逐軌檔**,L3 沒有對應輸出 → 兩條路實為分工。並存是刻意的,但沒寫下退場條件。建議寫進 `l3/README.md` 四條:① `SentinelProcessor`/`GEMSProcessor` 每個產品都有 l3 adapter 且過 HARP oracle;② `run_pipeline.py:184,223,303` 三個 `process_data()` 都有等價 `process_l3()` 並跑過整年;③ `wip_*` 無腳本依賴舊路徑檔案佈局;④ 舊路徑獨有能力有著落(CSV 逐站抽取、GeoTIFF、逐檔出圖)。四條打勾前舊路徑就是活的。
- **C16 三個 processor 掃檔邏輯統一**(B8 六份 → 一個共用函式)。會同時動三個 processor 核心迴圈,而它們零測試。**前置**:先為三個 `process_all_files` 建 golden-file 測試。估 >2 小時,獨立 session,且在 C1 上線後。
- **C17 ✅ `plot_taiwan_map` 兩份的處置**(2026-10-06 合併):wip 無人 import src 版(wip 裡的都是本地副本);`plot_taiwan.py` 單一實作,`'Taiwan'`=全島、中北部改名 `'Central'`,新增 `extent=`/`land_color=`/`figsize=`/`tight_layout=`,`'Global'` 不再報錯;電廠模組同名函式改為保留原輸出的棄用 wrapper。四種組合合併前後逐像素相同。
- **C18 ✅(2026-10-06,`932555f`)** `scripts/` 五處硬編路徑外部化:加 env 覆寫、保留現值當 fallback,既有指令不變。`download_epa_stations.py:100` 的跨 repo import 是另一層問題:那段 TLS 程式碼該屬於哪個 repo。

---

## 4. 待拍板

**D1 ✅(2026-09-20:都保留)AER_AI 要用哪個波段對?** `340_380`(processor 實際寫進檔案的)或 `354_388`(coverage 期待的)。兩者科學意義不同。也決定既有 AER_AI 處理檔要不要重跑。→ 影響 C3 是改一行還是重跑一批。

**D2 ✅(2026-09-20:六項全保留,O3T 200/400 保留)GEMS 產品清單以哪份為準?** catalog 沒有的 HCHO / SO2 / AERAOD / UVI 四項是還在用還是已停用?O3T 的 `200/400` 是刻意的科學選擇要保留,還是舊值?→ 影響 C10。

**D3 ✅(以 C1 最保守選項定案:只擋測試;mypy 留 C14)CI 要擋到什麼程度?** 只跑測試 / 測試 + lint / 再加 mypy。建議:先只擋測試(C1),mypy 限 `l3/` 與 `coverage/` 並 `continue-on-error`(C14)。→ 動 CI,會改變 PR 流程。

---

## 5. 建議執行順序

```
Phase 0  護欄(需 D3)          C1                                  ✅ 2026-09-09(採最保守選項:只跑測試)
Phase 1  低風險一批            C7 C19 C2 C4 C5 C6 C8 C20 C13     ✅ 2026-09-09 完成,commit 9ca01d1/34bf4be/78daa29
Phase 2  需先拍板              C3 ✅ C10 ✅                      2026-09-20 拍板「變數都保留給使用者」後完成,未 commit
Phase 3  先寫測試再改          C12 ✅ → C11 ✅ → C9 ✅          2026-09-10 完成,commit 3805b74/08895bd/4cde537
Phase 4  大項,各自獨立 session  C15 ✅ C18 ✅ C17 ✅ C14 ✅ | C16 擱置(等 C15 條件)
```

Phase 1 全部不改變任何輸出數值;Phase 3 每項都以「新增的測試先通過」為進入條件。

### Phase 0/1 完成紀錄(2026-09-09)

| # | 實際做法 | 驗證 |
|---|---|---|
| C1 | `pytest.yml` 加 `Run tests` step(`pytest -m "not requires_data"`),`.[test]` → `.[dev]` | 本機同指令 32 passed / 0 fail |
| C2 | `plot_nc.py:191` 改 `with Dataset(...) as _probe` 探測 group | 語法 + 全套測試 |
| C4 | `sentinel_processor.py` 取 config 前檢查 key,`ValueError` 列可處理產品;README `:145,245` 標「download only」 | 同上 |
| C5 | README:`/api`、S5P 樹補 `L2/`(raw/processed/figure 都有,logs flat)、MODIS processed 標 flat、Documentation 補三連結 | 目檢 |
| C6 | 兩份 MODIS docs 加「已於 55c4c5d 移除,現行入口 `merge_hdf_files_to_netcdf`」 | 目檢 |
| C7 | 刪 `requirements.txt` 的 `logging~=0.4.9.6` | — |
| C8 | `l3/README.md` 21 → 全套 32 | — |
| C13 | `RbfRegridder(qa_threshold=0.5)`,預設不變 | 全套測試 |
| C19 | `[tool.setuptools.packages.find] where=["."] include=["src*"]` | `find_packages` 收 9 個子套件 |
| C20 | `name = "satellite-s5p"`、authors = GitHub handle + email(要改真名說一聲) | `tomllib` 讀回 |
| **C12** | 先寫 `tests/test_utils_names.py`(13 項:S5P/MODIS/GEMS 檔名、時區、None、wrapper 契約、`pick_name`);`_pick` 抽到 `src/utils/nc_names.py`,兩處改 import;GEMS 兩個 wrapper 委派 `extract_datetime_from_filename`(無效日期仍回 None);移除兩個無用 `import re` | 新測試 13 passed;全套 46/37/0(無碟基準)|
| **C11** | 先寫 `tests/test_error_reporting.py`(4 項,caplog 斷言);`pipeline._name_sort_key` 退化發 warning(module logger);`regridder` 角點失敗空場 debug log;`core.py` 兩個裸 except 具名 + warning(module logger,不依賴 self.logger 順序);`sentinel_processor` 裸 except 具名 + `self.logger.warning`(存在檢查)| 4 passed;全套 50/37/0 |
| **C9** | 先寫 `tests/test_cli_regrid.py`(3 結構測試 monkeypatch `BASE_DIRS`/`_discover`/`regrid_to_series`;1 golden 標 `requires_data`,數字取自 2026-08-05 重構前實跑);`runner.regrid_to_series` 自己統計 `n_skipped` 寫進 attrs 與回傳;CLI `main` 改呼叫 runner,刪 `_make_adapter`/`BOUNDS` 副本,`BASE_DIRS` 抽成模組常數 | 3 passed + 1 skip(無碟);全套 53/38/0。**✅ golden 已於 2026-09-20 接碟跑過:PASSED**,重構後 CLI 對真實 GEMS 3 檔的輸出與 8/5 重構前逐項一致(251×201、finite 0.360、range 2.5e12~2.19e16);有碟全套 83/8/0 |

| **C3** | `ProductConfig` 加 `alt_dataset_names`;catalog `AER_AI` 預設仍 340_380(既有檔相容)+ alt 354_388;`SentinelProcessor(variable=…)` + `dataset_name` property(6 處呼叫改用,含寫檔 key 與 GeoTIFF);registry 值改候選 tuple、reader `_resolve_var` 依序取第一個存在的 | `test_product_configs.py` AER_AI 5 項 |
| **C10** | catalog 補 `GEMS_HCHO/SO2/AERAOD/UVI`、`GEMS_O3T` 改 200/400/viridis(processor 原值);`gems_processor.PRODUCTS` 改成 `PRODUCT_KEYS` 查表(友善名介面不變);runner `GEMS_RAW_DIR`/`SHORT_NAME` 補四項 | 先寫 golden(六項原值)再改,16 項;全套 104/8/0 |

★ **C12 的測試先跑就抓到一個真 bug(B24,新)**:`extract_datetime_from_filename` 的 S5P regex
`S5P_\w+_\w+__\w+_+(\d{8}T\d{6})_` 因 `\w` 含底線、貪婪回溯從最長開始,實際抓到的是**結束時間**
(`20220101T061009`),不是 docstring 承諾的開始時間(`20220101T042839`)。已改非貪婪並明確要求「_開始_結束_」。
影響範圍已查:pipeline 分日用 `gf.time`(granule 內時間)不受影響,排序同軌單調不變;只有 `core.l3_raw_files`
的日期篩選改用開始時間(更正確,年檔邊界最多差一軌)。

⚠️ 施工中的一個小雷:`pytest.yml` 末行沒有結尾換行,逐字 anchor 帶 `\n` 會對不上,改用 regex `$` 才成功。

---

## 6. 不動的東西(刻意的研究預設與設計)

- qa 門檻 0.5、超取樣 K=4、0.02° 網格、各產品 vmin/vmax/cmap、GEMS `mask_negative` 預設、TROPOMI 不濾負值。
- 舊 RBF 路徑與新 L3 路徑**並存**(C15 只寫條件,不刪)。
- `process_files_to_csv`、`save_as_tiff`、GeoTIFF 分支:呼叫點已註解並留說明,是刻意停用。
- `HimawariHub` mock、`RbfRegridder`:休眠但可能是備用,列出不刪。
- `wip_*` 目錄內容不評、不動;只守住它們依賴的 `src/` 介面(尤其 `plot_nc.basic_map`、`l3/adapters/gems.py::read`、`SupersampleBinRegridder`、`GridSpec.from_degrees`)。

---

*審視方法:codegraph explore(呼叫關係、blast radius)、grep 逐項核對行號、pytest / import / pip check 無副作用執行。P0 四項與 P1 三項(B5、B6、B19、B10)已人工獨立驗證;B10 描述據此修正。*
