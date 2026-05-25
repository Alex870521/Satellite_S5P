# MODIS HDF 文件合併功能

## 概述

這個功能允許您將多個 MODIS HDF 文件合併成一個 NetCDF 文件，創建一個包含時間、經度和緯度三個維度的 3D 數據集。

## 功能特點

- **跨平台支持**: 支持 macOS (使用 pyhdf) 和 Windows (使用 xarray)
- **多種合併模式**: 可按月份分別合併或全部合併到一個文件
- **自動日期過濾**: 支持指定日期範圍進行合併
- **數據驗證**: 自動檢查數據形狀一致性並進行調整
- **完整的元數據**: 保存詳細的數據集屬性和來源信息

## 支持的 MODIS 產品

- **MCD19A2**: MODIS Combined AOD Level 3 產品
- **MOD04_L2**: Terra MODIS AOD Level 2 產品  
- **MYD04_L2**: Aqua MODIS AOD Level 2 產品

## 使用方法

### 基本用法

```python
from src.processing.modis_processor import MODISProcessor

# 初始化處理器
processor = MODISProcessor()
processor.raw_dir = Path("data/raw")
processor.processed_dir = Path("data/processed")
processor.logger = logger
processor.file_type = "MCD19A2"

# 按月份合併文件
success = processor.merge_hdf_files_to_netcdf(
    pattern="**/MCD19A2/**/*.hdf",
    start_date="2024-01-01",
    end_date="2024-01-31",
    merge_by_month=True
)
```

### 參數說明

- `pattern` (str): 文件匹配模式，默認為 `"**/*.hdf"`
- `start_date` (str): 開始日期，格式為 'YYYY-MM-DD'
- `end_date` (str): 結束日期，格式為 'YYYY-MM-DD'
- `output_filename` (str): 輸出文件名，如果為 None 則自動生成
- `merge_by_month` (bool): 是否按月份分別合併

### 合併模式

#### 1. 按月份合併 (推薦)

```python
# 每月生成一個 NetCDF 文件
success = processor.merge_hdf_files_to_netcdf(
    start_date="2024-01-01",
    end_date="2024-03-31",
    merge_by_month=True
)
```

輸出文件結構：
```
data/processed/MCD19A2/
├── 2024/
│   ├── 01/
│   │   └── MCD19A2_merged_202401.nc
│   ├── 02/
│   │   └── MCD19A2_merged_202402.nc
│   └── 03/
│       └── MCD19A2_merged_202403.nc
```

#### 2. 全部合併

```python
# 所有文件合併到一個 NetCDF 文件
success = processor.merge_hdf_files_to_netcdf(
    start_date="2024-01-01",
    end_date="2024-03-31",
    merge_by_month=False
)
```

輸出文件：
```
data/processed/MCD19A2/MCD19A2_merged_20240101_20240331.nc
```

## 輸出數據格式

合併後的 NetCDF 文件包含以下結構：

### 維度
- `time`: 時間維度，對應每個 HDF 文件的日期
- `lat`: 緯度維度
- `lon`: 經度維度

### 變量
- `aod`: Aerosol Optical Depth 數據 (time, lat, lon)

### 座標
- `time`: 時間座標 (pandas datetime)
- `lat`: 緯度座標 (degrees_north)
- `lon`: 經度座標 (degrees_east)

### 屬性
- `title`: 數據集標題
- `description`: 數據集描述
- `creation_date`: 創建日期
- `source_files`: 源文件列表
- `file_type`: MODIS 產品類型

## 數據處理流程

1. **文件發現**: 根據模式找到所有符合條件的 HDF 文件
2. **日期過濾**: 根據指定的日期範圍過濾文件
3. **數據提取**: 從每個 HDF 文件中提取 AOD 數據和座標
4. **形狀檢查**: 確保所有數據具有相同的空間維度
5. **數據堆疊**: 將所有時間層的數據堆疊成 3D 數組
6. **NetCDF 創建**: 創建 xarray Dataset 並保存為 NetCDF 文件

## 示例腳本

參見 `examples/merge_modis_hdf_example.py` 了解完整的使用示例。

## 注意事項

1. **內存使用**: 合併大量文件時可能需要較多內存
2. **數據一致性**: 確保所有 HDF 文件使用相同的空間網格
3. **文件路徑**: 確保安裝了正確的輸入和輸出目錄路徑
4. **依賴項**: 確保安裝了必要的依賴項 (xarray, netcdf4, pandas)

## 故障排除

### 常見問題

1. **無法打開 HDF 文件**
   - 檢查文件路徑是否正確
   - 確保安裝了正確的 HDF 讀取庫

2. **數據形狀不一致**
   - 系統會自動調整到最小形狀
   - 檢查源文件是否來自不同的 MODIS tile

3. **內存不足**
   - 考慮使用按月份合併模式
   - 減少同時處理的文件數量

### 日誌信息

處理過程中會輸出詳細的日誌信息，包括：
- 找到的文件數量
- 處理進度
- 數據形狀信息
- 錯誤和警告信息

## 性能優化建議

1. **按月份合併**: 對於大量數據，建議使用按月份合併模式
2. **日期範圍**: 根據需要設置合適的日期範圍
3. **文件組織**: 將 HDF 文件按產品類型和日期組織在目錄中
