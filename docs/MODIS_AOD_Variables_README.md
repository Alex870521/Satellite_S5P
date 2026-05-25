# MODIS AOD 變量選擇指南

## 📊 概述

MODIS 處理器現在支持選擇不同的 AOD 變量，特別是針對 MOD04_L2 和 MYD04_L2 產品。

## 🎯 可用的 AOD 變量

### 1. **AOD_550_Dark_Target_Deep_Blue_Combined** (推薦 ⭐)
- **描述**: Dark Target (DT) 和 Deep Blue (DB) 算法的合併產品
- **波長**: 550 nm
- **質量**: 最高質量，綜合了兩種算法的優勢
- **覆蓋範圍**: 
  - DT: 適用於植被覆蓋區域和海洋
  - DB: 適用於明亮地表（如沙漠）
  - Combined: 提供最廣泛的空間覆蓋
- **推薦用途**: 
  - ✅ 科學研究和定量分析
  - ✅ 長期趨勢分析
  - ✅ 需要高質量數據的應用

### 2. **Optical_Depth_Land_And_Ocean** (舊版)
- **描述**: 早期版本的 AOD 產品
- **波長**: 通常為 550 nm
- **質量**: 標準質量
- **覆蓋範圍**: 陸地和海洋
- **推薦用途**:
  - ✅ 向後兼容舊代碼
  - ✅ 快速查看和初步分析
  - ⚠️ 數據質量可能不如 Combined 產品

### 3. **Image_Optical_Depth_Land_And_Ocean** (繪圖用)
- **描述**: 低解析度的可視化產品
- **質量**: 較低，主要用於快速可視化
- **推薦用途**:
  - ✅ 快速預覽
  - ✅ 初步數據檢查
  - ❌ 不推薦用於定量分析

## 🔧 使用方法

### 方法 1: 在代碼中直接指定

```python
from src.processing.modis_processor import MODISProcessor

# 使用推薦的 DT+DB 合併產品（預設）
processor = MODISProcessor(aod_variable='AOD_550_Dark_Target_Deep_Blue_Combined')

# 或使用舊版產品
processor = MODISProcessor(aod_variable='Optical_Depth_Land_And_Ocean')

# 或使用繪圖產品
processor = MODISProcessor(aod_variable='Image_Optical_Depth_Land_And_Ocean')
```

### 方法 2: 使用合併腳本

```bash
cd merge_data/MODIS
python merge_modis_hdf_example.py
```

運行時，腳本會詢問：
```
請選擇 AOD 變量:
  1. AOD_550_Dark_Target_Deep_Blue_Combined (推薦，DT+DB合併)
  2. Optical_Depth_Land_And_Ocean (舊版)
  3. Image_Optical_Depth_Land_And_Ocean (繪圖用)
請輸入選項 (1-3, 預設: 1):
```

### 方法 3: 在 merge_modis_data 函數中指定

```python
from merge_data.MODIS.merge_modis_hdf_example import merge_modis_data

# 使用 DT+DB 合併產品
success = merge_modis_data(
    data_type='MOD04_L2',
    start_date='2022-01-01',
    end_date='2022-12-31',
    aod_variable='AOD_550_Dark_Target_Deep_Blue_Combined'
)
```

## 🔄 自動備用機制

如果指定的 AOD 變量在文件中不存在，處理器會自動嘗試備用選項：

1. 首先嘗試: `AOD_550_Dark_Target_Deep_Blue_Combined`
2. 然後嘗試: `Optical_Depth_Land_And_Ocean`
3. 最後嘗試: `Image_Optical_Depth_Land_And_Ocean`

這確保了最大的數據兼容性。

## 📋 數據產品比較

| 變量名稱 | 質量 | 覆蓋範圍 | 科學用途 | 處理速度 |
|---------|------|---------|---------|---------|
| AOD_550_DT_DB_Combined | ⭐⭐⭐⭐⭐ | 最廣 | ✅ 推薦 | 中等 |
| Optical_Depth_Land_And_Ocean | ⭐⭐⭐ | 標準 | ⚠️ 可用 | 快 |
| Image_Optical_Depth_Land_And_Ocean | ⭐⭐ | 標準 | ❌ 不推薦 | 最快 |

## 🌍 適用產品

這些 AOD 變量選項**僅適用於**:
- ✅ MOD04_L2 (Terra MODIS Level 2)
- ✅ MYD04_L2 (Aqua MODIS Level 2)

**不適用於**:
- ❌ MCD19A2 (使用固定的 `Optical_Depth_047` 或 `Optical_Depth_055`)

## 📚 參考資料

- [MODIS Atmosphere Products](https://modis-atmosphere.gsfc.nasa.gov/)
- [MOD04_L2 Product Guide](https://modis-atmos.gsfc.nasa.gov/products/aerosol/mod04.html)
- [Dark Target vs Deep Blue Algorithm](https://darktarget.gsfc.nasa.gov/)

## 🔍 常見問題

### Q: 為什麼推薦使用 AOD_550_Dark_Target_Deep_Blue_Combined？
A: 這個產品結合了 Dark Target 和 Deep Blue 兩種算法，提供了最好的空間覆蓋和數據質量，特別適合台灣這種地形複雜的區域。

### Q: 如果我的舊代碼使用 Optical_Depth_Land_And_Ocean，需要更新嗎？
A: 不需要。處理器預設使用 AOD_550_DT_DB_Combined，但如果該變量不存在，會自動備用到 Optical_Depth_Land_And_Ocean。

### Q: 三個變量的數據值會不同嗎？
A: 是的。Combined 產品通常提供更準確的 AOD 值，特別是在明亮地表區域。建議使用 Combined 產品以獲得最佳結果。

### Q: 處理速度有差異嗎？
A: Combined 產品可能稍慢，因為它合併了兩種算法的結果。但對於科學研究來說，這種額外的處理時間是值得的。

## 📝 更新日誌

- **2025-10-14**: 
  - 添加 `aod_variable` 參數到 `MODISProcessor.__init__()`
  - 預設使用 `AOD_550_Dark_Target_Deep_Blue_Combined`
  - 實現自動備用機制
  - 更新 `merge_modis_hdf_example.py` 以支持變量選擇

## 💡 建議

對於大多數用戶，我們建議：
1. ✅ 使用預設設置（不指定 `aod_variable` 參數）
2. ✅ 讓處理器自動選擇最佳可用變量
3. ✅ 檢查日誌輸出以確認使用的變量

這樣可以確保獲得最高質量的數據，同時保持向後兼容性。

