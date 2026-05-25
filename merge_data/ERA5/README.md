# ERA5 進階合併工具

## 🌪️ 概述

`AdvancedERA5Merger` 是一個專門針對 ERA5 資料的強大合併工具，支援多種合併策略：

1. **多檔案時間合併** - 合併多個時間段的檔案
2. **多變數合併** - 合併同時間不同變數的檔案  
3. **壓力層合併** - 合併不同壓力層的檔案
4. **混合合併** - 複雜的組合合併策略

## 🚀 快速開始

### 互動式使用
```bash
python merge_data/ERA5/advanced_era5_merger.py
```

### 程式化使用
```python
from merge_data.ERA5.advanced_era5_merger import AdvancedERA5Merger

merger = AdvancedERA5Merger()

# 多檔案時間合併
file_paths = [Path("file1.nc"), Path("file2.nc"), Path("file3.nc")]
success = merger.merge_multiple_temporal(file_paths, Path("output.nc"))

# 多變數合併
success = merger.merge_multiple_variables(file_paths, Path("output.nc"))
```

## 📊 ERA5 資料類型支援

### 單層資料 (Single Level)
- **10米風速**: `u10`, `v10`
- **2米溫度**: `t2m`
- **邊界層高度**: `blh`
- **相對濕度**: `rh`
- **地表壓力**: `sp`

### 壓力層資料 (Pressure Level)
- **850, 700, 500, 300, 200, 150, 100 hPa** 等

## 🔄 合併策略

### 1. 時間序列合併 (`merge_multiple_temporal`)

**用途**: 合併多個時間段的相同變數

**範例**:
```python
# 合併2023年各季度的風速資料
file_paths = [
    Path("era5_sfc_10m_10m_20230101_20230331.nc"),  # Q1
    Path("era5_sfc_10m_10m_20230401_20230630.nc"),  # Q2
    Path("era5_sfc_10m_10m_20230701_20230930.nc"),  # Q3
    Path("era5_sfc_10m_10m_20231001_20231231.nc")   # Q4
]

success = merger.merge_multiple_temporal(
    file_paths, 
    Path("era5_wind_2023_full.nc")
)
```

### 2. 多變數合併 (`merge_multiple_variables`)

**用途**: 合併同時間的不同 ERA5 變數

**範例**:
```python
# 合併多種氣象要素
file_paths = [
    Path("era5_sfc_10m_10m_20230101_20231231.nc"),  # 風速 (u10, v10)
    Path("era5_sfc_2m__20230101_20231231.nc"),      # 溫度 (t2m)
    Path("era5_sfc_blh_20230101_20231231.nc"),      # 邊界層高度 (blh)
    Path("era5_sfc_rh__20230101_20231231.nc")       # 相對濕度 (rh)
]

success = merger.merge_multiple_variables(
    file_paths,
    Path("era5_comprehensive_2023.nc")
)
```

### 3. 壓力層合併 (`merge_multiple_variables`)

**用途**: 合併不同壓力層的相同變數

**範例**:
```python
# 合併多個壓力層的溫度資料
file_paths = [
    Path("era5_pressure_t_850_20230101_20231231.nc"),  # 850 hPa
    Path("era5_pressure_t_700_20230101_20231231.nc"),  # 700 hPa
    Path("era5_pressure_t_500_20230101_20231231.nc"),  # 500 hPa
    Path("era5_pressure_t_300_20230101_20231231.nc")   # 300 hPa
]

success = merger.merge_multiple_variables(
    file_paths,
    Path("era5_temperature_profile_2023.nc")
)
```

### 4. 混合合併 (`merge_hybrid`)

**用途**: 先按組別合併，再合併不同組別

**範例**:
```python
# 定義檔案組別
file_groups = {
    "風場組": [
        Path("era5_wind_2023_Q1.nc"),
        Path("era5_wind_2023_Q2.nc")
    ],
    "溫度組": [
        Path("era5_temp_2023_Q1.nc"),
        Path("era5_temp_2023_Q2.nc")
    ]
}

output_path = Path("era5_hybrid_merged.nc")
success = merger.merge_hybrid(file_groups, output_path, merge_strategy="temporal")
```

## 🎯 常見使用場景

### 場景1: 創建年度風場資料集
```python
# 合併12個月的風速資料
monthly_files = []
for month in range(1, 13):
    month_str = f"{month:02d}"
    file_path = Path(f"era5_sfc_10m_10m_2023{month_str}01_2023{month_str}31.nc")
    monthly_files.append(file_path)

success = merger.merge_multiple_temporal(
    monthly_files,
    Path("era5_wind_2023_annual.nc")
)
```

### 場景2: 創建綜合氣象資料集
```python
# 合併所有基本氣象要素
meteorological_files = [
    Path("era5_sfc_10m_10m_20230101_20231231.nc"),  # 風速
    Path("era5_sfc_2m__20230101_20231231.nc"),      # 溫度
    Path("era5_sfc_blh_20230101_20231231.nc"),      # 邊界層高度
    Path("era5_sfc_rh__20230101_20231231.nc"),      # 濕度
    Path("era5_sfc_sp__20230101_20231231.nc")       # 地表壓力
]

success = merger.merge_multiple_variables(
    meteorological_files,
    Path("era5_meteorology_2023.nc")
)
```

### 場景3: 創建大氣垂直剖面
```python
# 合併多個壓力層的風速資料
pressure_levels = [1000, 925, 850, 700, 600, 500, 400, 300, 250, 200, 150, 100, 70, 50, 30, 20, 10]

pressure_files = []
for level in pressure_levels:
    file_path = Path(f"era5_pressure_u_{level}_20230101_20231231.nc")
    pressure_files.append(file_path)

success = merger.merge_multiple_variables(
    pressure_files,
    Path("era5_wind_profile_2023.nc")
)
```

## 🔧 進階功能

### 檔案分析
```python
# 分析檔案資訊
analysis = merger.analyze_files(file_paths)
print(f"變數列表: {list(analysis['variables'])}")
print(f"時間範圍: {analysis['time_ranges']}")
```

### 變數名稱映射
```python
# 合併時重新命名變數
variable_mapping = {
    'wind_u': 'u10',
    'wind_v': 'v10',
    'temperature': 't2m',
    'boundary_layer_height': 'blh'
}

success = merger.merge_multiple_variables(
    file_paths, output_path, 
    variable_mapping=variable_mapping
)
```

## 📁 檔案結構

```
merge_data/
├── ERA5/                        # ERA5 合併工具套件
│   ├── __init__.py
│   ├── advanced_era5_merger.py  # 主要合併工具
│   ├── era5_merge_examples.py   # 使用範例
│   ├── test_advanced_merger.py  # 測試腳本
│   └── README.md               # 本說明文件
├── merge_modis_hdf_example.py   # MODIS 合併範例
├── merge_s5p_no2_example.py     # S5P NO2 合併範例
├── merge_s5p_o3_example.py      # S5P O3 合併範例
└── merge_s5p_so2_example.py     # S5P SO2 合併範例
```

## ⚡ 快速測試

```bash
# 測試基本功能
python merge_data/ERA5/test_advanced_merger.py

# 查看使用範例
python merge_data/ERA5/era5_merge_examples.py
```

## 💡 使用技巧

1. **選擇合適的合併策略**:
   - 時間連續 → 時間合併
   - 變數不同 → 變數合併
   - 壓力層不同 → 壓力層合併

2. **檔案準備**:
   - 確保檔案路徑正確
   - 檢查檔案格式一致
   - 確認時間維度對齊

3. **記憶體管理**:
   - 大量檔案使用混合合併
   - 分批處理避免記憶體不足
   - 合併後自動清理臨時檔案

## 📋 合併類型對照表

| 合併類型      | 適用場景       | 方法名稱                       | 範例                   |
|-----------|------------|----------------------------|----------------------|
| **時間合併**  | 多個時間段的相同變數 | `merge_multiple_temporal`  | 合併 Q1, Q2, Q3 的風速資料  |
| **變數合併**  | 同時間的不同變數   | `merge_multiple_variables` | 合併風速、溫度、濕度           |
| **壓力層合併** | 同時間的不同壓力層  | `merge_multiple_variables` | 合併 850, 700, 500 hPa |
| **混合合併**  | 複雜組合需求     | `merge_hybrid`             | 先分組合併，再跨組合併          |

## ⚠️ 注意事項

1. **檔案格式**: 所有檔案必須是 NetCDF 格式
2. **時間維度**: 預設使用 `valid_time` 作為時間維度
3. **重複處理**: 時間合併會自動處理重複時間點
4. **路徑設定**: 確保輸出目錄存在或有寫入權限
5. **記憶體限制**: 大量檔案合併時注意系統記憶體
6. **空間網格**: 確認所有檔案使用相同的空間網格 (0.25° x 0.25°)

## 🆚 與原始工具比較

| 功能   | 原始工具  | 進階工具   |
|------|-------|--------|
| 檔案數量 | 2個    | 多個     |
| 合併類型 | 僅時間合併 | 4種合併策略 |
| 互動介面 | 基本    | 進階     |
| 錯誤處理 | 基本    | 完整     |
| 檔案分析 | 無     | 有      |
| 變數映射 | 無     | 有      |
| 混合合併 | 無     | 有      |

## 📞 支援

如有問題，請：
1. 執行測試：`python merge_data/ERA5/test_advanced_merger.py`
2. 查看範例：`python merge_data/ERA5/era5_merge_examples.py`
3. 檢查本說明文件
