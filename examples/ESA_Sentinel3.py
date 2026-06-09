"""
SENTINEL-3 數據處理主程序

這個程序展示了如何使用 SENTINEL3Hub 來下載和處理 Sentinel-3 衛星數據。
支持多種海洋、陸地和大氣觀測數據的獲取、下載、處理和可視化。

注意 — Sentinel 家族定位(避免混淆):
- Sentinel-3(本範例):海/陸色彩(OLCI)、地表溫度(SLSTR)、高度計(SRAL)等,
  屬海洋/陸地任務,「非空氣品質」;研究上 SLSTR SWIR 曾用於甲烷(CH4)偵測。
- 空氣品質(NO2/O3/SO2/CH4):用 LEO 的 Sentinel-5P / TROPOMI(見 ESA_Sentinel5P.py)。
- 東亞「同步軌道、小時級」空品:用韓國 GEMS / GK-2B(見 NESC_GEMS.py)。
- Sentinel-4:同步軌道空品儀(搭載 MTG-S),目前尚在測試校驗、未開放;且觀測涵蓋
  歐洲/北非,「照不到台灣/東亞」,本專案不適用。

主要功能：
1. 從 Copernicus 數據中心獲取產品列表
2. 並行下載衛星數據文件
3. 處理數據並生成可視化圖像
4. 支持多種產品類型和處理級別

支持的儀器和產品類型：
- OLCI (Ocean and Land Colour Instrument)：海洋和陸地色彩儀
  * OL_1_EFR___: Level 1 全分辨率 (300m)
  * OL_1_ERR___: Level 1 降低分辨率 (1.2km)
  * OL_2_LFR___: Level 2 陸地全分辨率
  * OL_2_WFR___: Level 2 水體全分辨率

- SLSTR (Sea and Land Surface Temperature Radiometer)：海陸表面溫度輻射計
  * SL_1_RBT___: Level 1 輻射亮度和溫度
  * SL_2_LST___: Level 2 陸地表面溫度
  * SL_2_WST___: Level 2 水體表面溫度
  * SL_2_FRP___: Level 2 火點偵測

- SRAL (SAR Radar Altimeter)：SAR 雷達高度計
  * SR_1_SRA___: Level 1 SAR
  * SR_2_LAN___: Level 2 陸地

處理級別：
- NTC: 近實時合併 (Near Real-Time Consolidated)
- NRT: 近實時 (Near Real-Time) - 最快但精度較低
- STC: 短時效合併 (Short Time Critical)
- NTC: 非時效合併 (Non Time Critical) - 標準處理，平衡速度和精度

使用前請確保：
1. 設置環境變量 COPERNICUS_USERNAME 和 COPERNICUS_PASSWORD
2. 確保有足夠的磁盤空間存儲數據 (Sentinel-3 文件通常較大)
3. 根據需要調整 max_workers 參數
"""

from datetime import datetime
from src.api import SENTINEL3Hub


def main():
    """
    SENTINEL-3 數據處理主函數

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
    start_date, end_date = datetime(2025, 1, 1), datetime(2025, 1, 31)

    # 文件處理類別選擇
    # 選項說明：
    # - 'NRT': 近實時 (Near Real-Time) - 最快但精度較低，適合即時監測
    # - 'NTC': 近實時合併 (Near Real-Time Consolidated) - 較好精度
    # - 'STC': 短時效合併 (Short Time Critical) - 標準處理
    # - 'NT': 非時效 (Non Time Critical) - 最高精度，用於科學研究
    file_class = 'NT'   # OLCI/SLSTR 用 'NT'(科學級非時效)或 'NR'(近即時);勿用 NTC(對 OLCI 無效會回 0 筆)

    # 產品類型選擇
    # OLCI 產品選項：
    # - 'OL_1_EFR___': OLCI Level 1 全分辨率 - 適合高精度分析
    # - 'OL_1_ERR___': OLCI Level 1 降低分辨率 - 適合快速處理
    # - 'OL_2_LFR___': OLCI Level 2 陸地全分辨率 - 陸地監測
    # - 'OL_2_WFR___': OLCI Level 2 水體全分辨率 - 海洋/水體監測
    # - 'OL_2_LRR___': OLCI Level 2 陸地降低分辨率
    # - 'OL_2_WRR___': OLCI Level 2 水體降低分辨率
    #
    # SLSTR 產品選項：
    # - 'SL_1_RBT___': SLSTR Level 1 輻射亮度 - 輻射校正數據
    # - 'SL_2_LST___': SLSTR Level 2 陸地表面溫度 - 地表溫度監測
    # - 'SL_2_WST___': SLSTR Level 2 海表溫度 - 海洋溫度監測
    # - 'SL_2_FRP___': SLSTR Level 2 火點 - 森林火災監測
    #
    # SRAL 產品選項：
    # - 'SR_1_SRA___': SRAL Level 1 - 雷達高度計原始數據
    # - 'SR_2_LAN___': SRAL Level 2 陸地 - 陸地高度測量
    # - 'SR_2_WAT___': SRAL Level 2 水體 - 海洋高度測量
    file_type = 'OL_2_WFR___'  # 以 OLCI 水體全分辨率為例

    # 地理邊界設置 (經度範圍, 緯度範圍)
    # 格式：(min_lon, max_lon, min_lat, max_lat)
    # 這裡設置為台灣周邊海域
    boundary = (119, 123, 21, 26)

    # =============================================================================
    # 2. 創建數據中心實例
    # =============================================================================

    # 創建 SENTINEL3Hub 實例
    # max_workers: 並行下載的線程數，建議根據網絡帶寬調整
    # 注意：Sentinel-3 文件較大，建議使用較少的線程數 (2-3個)
    sentinel_hub = SENTINEL3Hub(max_workers=2)

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
        limit=10                   # 限制產品數量（可選）
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
    # 注意：Sentinel-3 文件通常較大 (數百MB到數GB)，下載可能需要較長時間
    sentinel_hub.download_data(products, show_progress=True)

    # =============================================================================
    # 5. 處理數據
    # =============================================================================

    print("⚙️ 開始處理數據...")
    # 處理下載的數據並生成可視化圖像
    # 這會創建處理後的 NetCDF 文件和圖像
    sentinel_hub.process_data()

    print("✅ 數據處理完成！")


def example_usage():
    """
    使用示例和配置說明

    這個函數展示了不同的使用場景和配置選項
    """
    print("=" * 80)
    print("SENTINEL-3 數據處理使用示例")
    print("=" * 80)

    print("\n📋 支持的產品類型:")
    product_types = {
        "OLCI 海洋色彩": [
            "OL_1_EFR___  - Level 1 全分辨率 (300m)",
            "OL_1_ERR___  - Level 1 降低分辨率 (1.2km)",
            "OL_2_LFR___  - Level 2 陸地全分辨率",
            "OL_2_WFR___  - Level 2 水體全分辨率",
            "OL_2_LRR___  - Level 2 陸地降低分辨率",
            "OL_2_WRR___  - Level 2 水體降低分辨率"
        ],
        "SLSTR 溫度輻射": [
            "SL_1_RBT___  - Level 1 輻射亮度和溫度",
            "SL_2_LST___  - Level 2 陸地表面溫度",
            "SL_2_WST___  - Level 2 海表溫度",
            "SL_2_FRP___  - Level 2 火點偵測"
        ],
        "SRAL 雷達高度": [
            "SR_1_SRA___  - Level 1 SAR 雷達",
            "SR_2_LAN___  - Level 2 陸地高度",
            "SR_2_WAT___  - Level 2 海洋高度"
        ]
    }

    for category, types in product_types.items():
        print(f"\n  {category}:")
        for product_type in types:
            print(f"    • {product_type}")

    print("\n⚙️ 處理類別選擇:")
    print("  NRT: 近實時處理 - 最快但精度較低，適合即時監測")
    print("  NTC: 近實時合併 - 較好精度，適合一般應用 (推薦)")
    print("  STC: 短時效合併 - 標準處理，平衡速度和精度")
    print("  NT:  非時效處理 - 最高精度，用於科學研究")

    print("\n🌍 地理邊界設置示例:")
    boundaries = {
        "台灣周邊海域": (119, 123, 21, 26),
        "台灣海峽": (117, 121, 23, 26),
        "東海": (120, 128, 25, 33),
        "南海北部": (110, 120, 10, 23),
        "西太平洋": (120, 150, 20, 40)
    }

    for region, boundary in boundaries.items():
        print(f"  {region}: {boundary}")

    print("\n💡 應用場景建議:")
    print("  1. 海洋色彩監測 (葉綠素、藻華): 使用 OL_2_WFR___ (OLCI 水體)")
    print("  2. 陸地植被監測 (NDVI): 使用 OL_2_LFR___ (OLCI 陸地)")
    print("  3. 海表溫度分析: 使用 SL_2_WST___ (SLSTR 海溫)")
    print("  4. 地表溫度分析 (熱島效應): 使用 SL_2_LST___ (SLSTR 陸溫)")
    print("  5. 火災監測: 使用 SL_2_FRP___ (SLSTR 火點)")
    print("  6. 海洋高度/海平面變化: 使用 SR_2_WAT___ (SRAL 海洋)")

    print("\n🔬 研究案例:")
    print("  • 海洋研究: OL_2_WFR___ 監測台灣海峽藻華現象")
    print("  • 氣候研究: SL_2_WST___ 分析黑潮海溫變化")
    print("  • 農業研究: OL_2_LFR___ 監測農田植被健康度")
    print("  • 都市研究: SL_2_LST___ 分析都市熱島效應")
    print("  • 防災研究: SL_2_FRP___ 即時監測森林火災")

    print("\n📊 數據特性:")
    print("  OLCI:")
    print("    - 空間解析度: 300m (全分辨率) / 1.2km (降低分辨率)")
    print("    - 光譜範圍: 21 個波段 (400-1020 nm)")
    print("    - 覆蓋寬度: 1270 km")
    print("  SLSTR:")
    print("    - 空間解析度: 500m (熱紅外) / 1km (短波紅外)")
    print("    - 溫度精度: 0.3K (海溫)")
    print("    - 覆蓋寬度: 1420 km (雙視角)")
    print("  SRAL:")
    print("    - 測量精度: <2 cm (海洋高度)")
    print("    - 覆蓋寬度: 沿軌道 (非成像)")

    print("\n⚠️ 注意事項:")
    print("  1. Sentinel-3 文件較大 (通常數百MB到數GB)，下載需要較長時間")
    print("  2. 確保有足夠的磁盤空間 (建議預留 10GB 以上)")
    print("  3. OLCI Level 2 產品已包含大氣校正和地理定位")
    print("  4. SLSTR 數據適合長時間序列分析")
    print("  5. SRAL 數據為沿軌道數據，非成像數據")
    print("  6. 建議使用較少的並行下載線程 (2-3個) 以避免超載")


def advanced_example():
    """
    進階使用範例：多類型產品組合分析
    """
    print("\n" + "=" * 80)
    print("進階範例：海洋環境綜合監測")
    print("=" * 80)

    # 組合使用 OLCI 和 SLSTR 進行海洋環境監測
    print("\n此範例展示如何組合使用不同產品進行綜合分析：")
    print("1. OLCI OL_2_WFR___ - 監測葉綠素濃度和水色")
    print("2. SLSTR SL_2_WST___ - 監測海表溫度")
    print("\n這種組合可以用於：")
    print("  • 研究海溫對藻華的影響")
    print("  • 監測海洋熱浪事件")
    print("  • 分析洋流和水團變化")

    print("\n範例程式碼：")
    print("""
    # 1. 下載 OLCI 水色數據
    sentinel_hub = SENTINEL3Hub(max_workers=2)
    olci_products = sentinel_hub.fetch_data(
        file_class='NTC',
        file_type='OL_2_WFR___',
        start_date=datetime(2024, 6, 1),
        end_date=datetime(2024, 6, 30),
        boundary=(119, 123, 21, 26)
    )
    sentinel_hub.download_data(olci_products, show_progress=True)

    # 2. 下載 SLSTR 海溫數據
    slstr_products = sentinel_hub.fetch_data(
        file_class='NTC',
        file_type='SL_2_WST___',
        start_date=datetime(2024, 6, 1),
        end_date=datetime(2024, 6, 30),
        boundary=(119, 123, 21, 26)
    )
    sentinel_hub.download_data(slstr_products, show_progress=True)

    # 3. 處理並分析數據
    sentinel_hub.process_data()
    """)


if __name__ == "__main__":
    # 顯示使用說明
    example_usage()

    print("\n" + "=" * 80)
    input("按 Enter 鍵開始執行主程序...")
    print("=" * 80 + "\n")

    # 運行主程序
    main()

    # 顯示進階範例
    advanced_example()
