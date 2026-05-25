# Sentinel-5P 統一合併工具

這個工具可以合併 Sentinel-5P 的 NO2、O3、SO2 等多種氣體數據。

## 新功能

### 支援兩種檔案類型

1. **原始檔案** (預設)
   - 需要重投影和插值到統一網格
   - 適用於從衛星原始數據開始處理

2. **已處理檔案** (新功能)
   - 已經插值到統一網格的檔案
   - 直接合併，無需重複插值
   - 處理速度更快

## 使用方法

### 互動式使用

```bash
python merge_s5p_unified.py
```

程式會引導您選擇：
1. 檔案類型 (原始/已處理)
2. 氣體類型 (NO2/O3/SO2)
3. 年份和日期範圍

### 批次處理

```bash
python merge_s5p_unified.py --batch
```

批次處理多種氣體，同樣可以選擇檔案類型。

## 檔案結構

### 輸入檔案路徑

#### 原始檔案 (需要重投影和插值)
```
/Volumes/Transcend/Sentinel-5P/raw/
├── NO2___/
│   └── 2024/
│       └── 01/
│           └── *.nc
├── O3____/
└── SO2___/
```

#### 已處理檔案 (已經插值到統一網格)
```
/Users/chanchihyu/DataCenter/Satellite/Sentinel-5P/processed/
├── NO2___/
│   └── 2024/
│       └── 01/
│           └── *.nc
├── O3____/
└── SO2___/
```

### 輸出檔案路徑

所有合併後的 NetCDF 檔案都會保存到桌面：
```
/Users/chanchihyu/Desktop/
└── S5P_{GAS_TYPE}_{START_DATE}_{END_DATE}.nc
```

例如：`S5P_NO2_20240101_20240131.nc`

## 路徑設定邏輯

- **`raw_dir`**: 輸入檔案的位置
  - 原始檔案：`/Volumes/Transcend/Sentinel-5P/raw/`
  - 已處理檔案：`/Users/chanchihyu/DataCenter/Satellite/Sentinel-5P/processed/`

- **`output_dir`**: 輸出檔案的位置
  - 統一輸出到：`/Users/chanchihyu/Desktop/`

## 技術細節

### 原始檔案處理
- 讀取 PRODUCT group 中的數據
- 應用 QA 過濾 (閾值: 0.75)
- 重投影到台灣統一網格 (0.01° 解析度)
- 插值處理

### 已處理檔案處理
- 直接讀取已插值的數據
- 跳過重投影步驟
- 直接合併到時間序列

## 配置

可以在 `GAS_CONFIGS` 和 `PROCESSED_CONFIGS` 中修改：
- 檔案路徑模式
- 變數名稱
- 輸出變數名稱
- 單位和描述