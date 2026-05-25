# GEMS和Himawari-8 API使用說明

本文檔介紹如何使用新創建的GEMS和Himawari-8衛星數據API。

## 概述

基於現有的`SatelliteHub`基類，我們新增了兩個衛星API：

1. **GEMS API** (`GEMSHub`) - 韓國大氣環境監測衛星
2. **Himawari API** (`HimawariHub`) - 日本氣象衛星

## GEMS API

### 簡介
GEMS (Geostationary Environment Monitoring Spectrometer) 是韓國的大氣環境監測衛星，主要用於監測東亞地區的大氣污染物。

### 支持的產品類型
- `NO2` - 二氧化氮
- `O3` - 臭氧
- `HCHO` - 甲醛
- `SO2` - 二氧化硫
- `AOD` - 氣溶膠光學厚度
- `CO` - 一氧化碳
- `CH4` - 甲烷

### 基本使用

```python
from src.api.gems_api import GEMSHub
from datetime import datetime, timedelta

# 創建GEMS API實例
gems = GEMSHub()

# 查詢NO2數據
start_date = datetime.now() - timedelta(days=7)
end_date = datetime.now()
boundary = (120.0, 22.0, 122.0, 25.0)  # 台灣附近區域

products = gems.fetch_data(
    product_type='NO2',
    start_date=start_date,
    end_date=end_date,
    boundary=boundary,
    limit=10
)

# 下載數據
if products:
    downloaded_files = gems.download_data(products, show_progress=True)
    
    # 處理數據
    processed_files = gems.process_data()
```

### 產品信息查詢

```python
# 獲取所有可用產品
products = gems.get_available_products()

# 獲取特定產品信息
no2_info = gems.get_product_info('NO2')
print(f"NO2: {no2_info['name']} - {no2_info['description']}")
```

## Himawari API

### 簡介
Himawari-8/9是日本氣象廳運營的地球同步氣象衛星，提供高分辨率的可見光和紅外圖像。

### 支持的產品類型
- `VIS` - 可見光
- `IR1` - 紅外1 (10.4μm)
- `IR2` - 紅外2 (8.6μm)
- `IR3` - 紅外3 (6.9μm)
- `IR4` - 紅外4 (13.3μm)
- `WV` - 水汽 (6.2μm)
- `BAND03` 到 `BAND16` - 多光譜波段

### 時間間隔選項
- `10min` - 10分鐘間隔
- `30min` - 30分鐘間隔
- `1hour` - 1小時間隔
- `3hour` - 3小時間隔
- `6hour` - 6小時間隔
- `12hour` - 12小時間隔
- `daily` - 每日

### 基本使用

```python
from src.api.himawari_api import HimawariHub
from datetime import datetime, timedelta

# 創建Himawari API實例
himawari = HimawariHub()

# 查詢可見光數據
start_date = datetime.now() - timedelta(hours=6)
end_date = datetime.now()
boundary = (120.0, 22.0, 122.0, 25.0)  # 台灣附近區域

products = himawari.fetch_data(
    product_type='VIS',
    start_date=start_date,
    end_date=end_date,
    boundary=boundary,
    time_interval='30min',
    resolution='half',
    limit=10
)

# 下載數據
if products:
    downloaded_files = himawari.download_data(products, show_progress=True)
    
    # 處理數據
    processed_files = himawari.process_data()
```

### 衛星信息查詢

```python
# 獲取衛星位置信息
position = himawari.get_satellite_position()
print(f"衛星: {position['satellite']}")
print(f"經度: {position['longitude']}")
print(f"緯度: {position['latitude']}")
print(f"高度: {position['altitude']} km")
print(f"覆蓋區域: {position['coverage_area']}")

# 獲取可用時間間隔
intervals = himawari.get_available_time_intervals()
```

## 目錄結構

兩個API都遵循統一的目錄結構：

```
BASE_DIR/
├── GEMS/                    # GEMS數據目錄
│   ├── logs/               # 日誌文件
│   ├── raw/                # 原始數據
│   │   ├── NO2/
│   │   ├── O3/
│   │   └── ...
│   ├── processed/          # 處理後數據
│   └── figure/             # 圖像文件
└── Himawari/               # Himawari數據目錄
    ├── logs/               # 日誌文件
    ├── raw/                # 原始數據
    │   ├── VIS/
    │   ├── IR1/
    │   └── ...
    ├── processed/          # 處理後數據
    └── figure/             # 圖像文件
```

## 環境變量設置

### GEMS API
```bash
export GEMS_USERNAME="your_username"
export GEMS_PASSWORD="your_password"
```

### Himawari API
```bash
export HIMAWARI_USERNAME="your_username"
export HIMAWARI_PASSWORD="your_password"
```

## 注意事項

1. **認證**: 目前API使用模擬認證，實際使用時需要設置相應的環境變量
2. **數據處理**: 數據處理功能需要實現相應的Processor類
3. **網絡連接**: 下載功能需要穩定的網絡連接
4. **存儲空間**: 衛星數據文件通常較大，請確保有足夠的存儲空間

## 示例腳本

完整的使用示例請參考：
- `examples/gems_himawari_example.py` - 基本使用示例
- `test_new_apis_simple.py` - 功能測試腳本

## 技術支持

如有問題，請檢查：
1. 虛擬環境是否已激活
2. 依賴包是否已安裝
3. 環境變量是否正確設置
4. 網絡連接是否正常
