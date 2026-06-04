# GEMS API 使用說明

本文檔介紹如何使用 GEMS 衛星數據 API（`GEMSHub`），基於現有的 `SatelliteHub` 基類。

## 簡介

GEMS (Geostationary Environment Monitoring Spectrometer) 是韓國的大氣環境監測衛星，主要用於監測東亞地區的大氣污染物。

## 支持的產品類型 (對應 NESC 產品代碼)

- `NO2` - 二氧化氮
- `O3` / `O3T` - 臭氧總量；`O3P` - 臭氧垂直分布
- `HCHO` - 甲醛
- `CHOCHO` - 乙二醛
- `SO2` - 二氧化硫
- `AOD` (`AERAOD`) - 氣溶膠光學厚度；`AEH` - 氣溶膠有效高度
- `UVI` - 紫外線指數；`CLOUD` - 雲資訊

> GEMS 為 UV-Vis 光譜儀，**不**量測 CO / CH4。資料層級：L2 (swath)、L3 (網格化日/月平均)、L4 (地面 PM)。資料涵蓋約 2020-09 起，白天每小時一檔。

## 基本使用

```python
from src.api.gems_api import GEMSHub

# 需先設定環境變數 GEMS_API_KEY（建議寫在專案根目錄 .env）
gems = GEMSHub()

# 查詢 NO2 檔案清單（GEMS 白天每小時一檔）
products = gems.fetch_data(
    product_type='NO2',          # 友善名或 NESC 代碼
    start_date='2023-05-15',
    end_date='2023-05-15',
    ver=None,                    # None = 自動取最新版（如 NO2 v4.0.1）
    level='L2',
    limit=10,
)

# 下載原始 swath（每檔約 270 MB，存到 $SATELLITE_BASE_DIR/GEMS/raw/...）
if products:
    gems.download_data(products, show_progress=True)

    # 網格化（QC + 內插到台灣網格）→ 標準 NetCDF + 圖 + 月動畫
    gems.process_data(start_date='2023-05-15', end_date='2023-05-15')
```

## 產品信息查詢

```python
# 獲取所有可用產品
products = gems.get_available_products()

# 獲取特定產品信息
no2_info = gems.get_product_info('NO2')
print(f"NO2: {no2_info['name']} - {no2_info['description']}")
```

## 目錄結構

```
BASE_DIR/
└── GEMS/                    # GEMS數據目錄
    ├── logs/               # 日誌文件
    ├── raw/                # 原始數據
    │   ├── NO2/
    │   ├── O3/
    │   └── ...
    ├── processed/          # 處理後數據
    └── figure/             # 圖像文件
```

## 環境變量設置

```bash
# 於 https://nesc.nier.go.kr 申請 Open-API key，設定「單一金鑰」：
export GEMS_API_KEY="api-xxxxxxxxxxxxxxxx"
```

## 注意事項

1. **認證**: GEMS 已接 NESC Open-API（需 `GEMS_API_KEY`，以 `getKeyInfo.do` 驗證）
2. **數據處理**: 已有 `GEMSProcessor`（QC→內插→NetCDF→繪圖→月動畫）
3. **網絡連接**: 下載功能需要穩定的網絡連接
4. **存儲空間**: swath 原始檔通常較大（每檔約 270 MB），請確保有足夠的存儲空間

## 技術支持

如有問題，請檢查：
1. 虛擬環境是否已激活
2. 依賴包是否已安裝
3. 環境變量 `GEMS_API_KEY` 是否正確設置
4. 網絡連接是否正常
