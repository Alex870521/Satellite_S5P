"""
SENTINEL-5P 數據處理主程序

這個程序展示了如何使用 SENTINEL5PHub 來下載和處理 Sentinel-5P 衛星數據。
支持多種大氣成分的數據獲取、下載、處理和可視化。

主要功能：
1. 從 Copernicus 數據中心獲取產品列表
2. 並行下載衛星數據文件
3. 處理數據並生成可視化圖像
4. 支持多種產品類型和處理級別

支持的產品類型：
- 臭氧產品：O₃ 總柱、對流層柱、剖面
- 氣體產品：NO₂, SO₂, CO, CH₄, HCHO
- 雲和氣溶膠產品：雲掩膜、氣溶膠指數等

處理級別：
- NRTI: 近實時處理 (最快但精度較低)
- OFFL: 離線處理 (標準處理，平衡速度和精度)
- RPRO: 重新處理 (最高精度，用於歷史數據)

使用前請確保：
1. 設置環境變量 COPERNICUS_USERNAME 和 COPERNICUS_PASSWORD
2. 確保有足夠的磁盤空間存儲數據
3. 根據需要調整 max_workers 參數
"""

from datetime import datetime
from src.api import SENTINEL5PHub


def main():
    """
    SENTINEL-5P 數據處理主函數
    
    這個函數展示了完整的數據處理流程：
    1. 設置參數
    2. 創建數據中心實例
    3. 獲取產品列表
    4. 下載數據
    5. 處理數據
    """
    # =============================================================================
    # 1. 設置參數
    # =============================================================================
    
    # 時間範圍設置
    start_date, end_date = datetime(2025, 1, 1), datetime(2025, 10, 31)
    
    # 文件處理類別選擇
    # 選項說明：
    # - 'NRTI': 近實時處理 (Near Real-Time) - 最快但精度較低，適合監測
    # - 'OFFL': 離線處理 (Offline) - 標準處理，平衡速度和精度
    # - 'RPRO': 重新處理 (Reprocessing) - 最高精度，用於歷史數據
    file_class = 'OFFL'  # 推薦使用 OFFL 進行一般應用
    
    # 產品類型選擇
    # 選項說明：
    # - 'NO2___': NO₂ 對流層柱 - 城市空氣品質監測
    # - 'O3____': O₃ 總垂直柱 - 臭氧監測
    # - 'CO____': CO 總柱 - 一氧化碳監測
    # - 'SO2___': SO₂ 總垂直柱 - 工業污染監測
    # - 'CH4___': CH₄ 甲烷混合比 - 溫室氣體監測
    # - 'HCHO__': HCHO 對流層垂直柱 - 揮發性有機物
    # - 'CLOUD_': 雲掩膜 - 雲檢測
    # - 'AER_AI': 氣溶膠指數 - 沙塵暴監測
    # - 'AER_LH': 氣溶膠層高 - 大氣層結構
    file_type = 'NO2___'  # 以 NO₂ 為例
    
    # 地理邊界設置 (經度範圍, 緯度範圍)
    # 格式：(min_lon, max_lon, min_lat, max_lat)
    # 這裡設置為台灣地區
    boundary = (120, 122, 22, 25)

    # =============================================================================
    # 2. 創建數據中心實例
    # =============================================================================
    
    # 創建 SENTINEL5PHub 實例
    # max_workers: 並行下載的線程數，建議根據網絡帶寬調整
    # 注意：過多的線程可能導致 API 限制，建議使用 3-5 個線程
    sentinel_hub = SENTINEL5PHub(max_workers=3)
    
    # =============================================================================
    # 3. 獲取產品列表
    # =============================================================================
    
    print("🔍 正在獲取產品列表...")
    products = sentinel_hub.fetch_data(
        file_class=file_class,      # 處理類別
        file_type=file_type,       # 產品類型
        start_date=start_date,     # 開始日期
        end_date=end_date,         # 結束日期
        boundary=boundary,         # 地理邊界
    )
    
    if not products:
        print("❌ 未找到符合條件的產品，請檢查參數設置")
        return
    
    print(f"✅ 找到 {len(products)} 個產品")
    
    # =============================================================================
    # 4. 下載數據
    # =============================================================================
    
    print("📥 開始下載數據...")
    # 下載所有產品文件
    # show_progress=True 會顯示下載進度條
    sentinel_hub.download_data(products, show_progress=True)
    
    # =============================================================================
    # 5. 處理數據
    # =============================================================================
    
    print("⚙️ 開始處理數據...")
    # 處理下載的數據並生成可視化圖像
    # 這會創建處理後的 NetCDF 文件和圖像
    sentinel_hub.process_data()
    
    print("✅ 數據處理完成！")
    
    # =============================================================================
    # 6. 可選：提取站點數據 (已註釋)
    # =============================================================================
    
    # 如果需要提取特定站點的數據，可以取消註釋以下代碼
    # 定義觀測站點
    # STATIONS = [
    #     {"name": "FS", "lat": 22.6294, "lon": 120.3461},  # 高雄鳳山
    #     {"name": "NZ", "lat": 22.7422, "lon": 120.3339},  # 高雄楠梓
    #     {"name": "QT", "lat": 22.7575, "lon": 120.3057},  # 橋頭
    #     {"name": "TH", "lat": 24.1817, "lon": 120.5956},  # 台中
    #     {"name": "TP", "lat": 25.0330, "lon": 121.5654}   # 台北
    # ]
    # 
    # # 批量處理並提取站點數據
    # csv_files = sentinel_hub.processor.process_files_to_csv(
    #     file_pattern="**/2024/*/*.nc",    # 文件模式
    #     stations=STATIONS,                # 站點列表
    #     start_date=start_date,           # 開始日期
    #     end_date=end_date,               # 結束日期
    #     fill_missing_dates=True,         # 填充缺失日期
    #     extract_surrounding=True         # 提取周圍數據
    # )

def example_usage():
    """
    使用示例和配置說明
    
    這個函數展示了不同的使用場景和配置選項
    """
    print("=" * 60)
    print("SENTINEL-5P 數據處理使用示例")
    print("=" * 60)
    
    print("\n📋 支持的產品類型:")
    product_types = {
        "臭氧產品": ["O3____", "O3_TCL", "O3__PR"],
        "氣體產品": ["NO2___", "SO2___", "CO____", "CH4___", "HCHO__"],
        "雲和氣溶膠": ["CLOUD_", "FRESCO", "AER_LH", "AER_AI"]
    }
    
    for category, types in product_types.items():
        print(f"  {category}: {', '.join(types)}")
    
    print("\n⚙️ 處理類別選擇:")
    print("  NRTI: 近實時處理 - 最快但精度較低，適合監測")
    print("  OFFL: 離線處理 - 標準處理，平衡速度和精度 (推薦)")
    print("  RPRO: 重新處理 - 最高精度，用於歷史數據")
    
    print("\n🌍 地理邊界設置示例:")
    boundaries = {
        "台灣": (120, 122, 22, 25),
        "台北": (121.3, 121.7, 24.9, 25.2),
        "高雄": (120.2, 120.4, 22.5, 22.8),
        "全台灣": (119.5, 122.5, 21.5, 25.5)
    }
    
    for region, boundary in boundaries.items():
        print(f"  {region}: {boundary}")
    
    print("\n💡 使用建議:")
    print("  1. 城市空氣品質監測: 使用 NO₂, O₃, SO₂ (高解析度)")
    print("  2. 區域性研究: 使用 CO, CH₄ (中等解析度)")
    print("  3. 全球性分析: 使用 O₃ 剖面 (低解析度)")
    print("  4. 工業污染監測: 使用 SO₂, NO₂ (高解析度)")
    print("  5. 溫室氣體研究: 使用 CH₄, CO (中等解析度)")
    
    print("\n⚠️ 注意事項:")
    print("  1. 確保設置環境變量 COPERNICUS_USERNAME 和 COPERNICUS_PASSWORD")
    print("  2. 確保有足夠的磁盤空間存儲數據")
    print("  3. 根據網絡帶寬調整 max_workers 參數")
    print("  4. 大範圍或長時間的數據可能需要較長處理時間")


if __name__ == "__main__":
    # 運行主程序
    main()