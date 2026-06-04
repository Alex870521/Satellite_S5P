"""
GEMS 數據處理主程序

展示如何使用 GEMSHub 從 NIER/NESC GEMS Open-API 下載與處理 GEMS 衛星數據。
GEMS 為韓國地球同步大氣環境監測衛星（GK-2B），白天每小時一檔，涵蓋東亞地區。

支持的產品類型：
- NO2 / O3 (O3T) / O3P / SO2 / HCHO / CHOCHO
- AOD (AERAOD) / AEH / UVI / CLOUD

使用前請確保：
1. 於 https://nesc.nier.go.kr 申請 Open-API key，於專案根目錄 .env 設定 GEMS_API_KEY
2. 確保有足夠的磁盤空間（原始 swath 每檔約 270 MB）
3. 根據網絡帶寬調整 max_workers 參數
"""
from src.api import GEMSHub


def main():
    """標準流程：fetch → download → process。"""
    # 1. 設定參數
    start_date, end_date = '2023-05-15', '2023-05-15'
    product_type = 'NO2'

    # 2. 建立 data hub（需 .env 內的 GEMS_API_KEY）
    gems_hub = GEMSHub()

    # 3. 查詢檔案清單（GEMS 白天每小時一檔）
    products = gems_hub.fetch_data(
        product_type=product_type,
        start_date=start_date,
        end_date=end_date,
        ver=None,        # None = 線上自動解析最新版（如 NO2 v4.0.1）
        level='L2',
    )

    # 4. 下載原始 swath
    if products:
        gems_hub.download_data(products)

        # 5. 網格化（QC → 內插到台灣網格）→ NetCDF + 圖 + 月動畫
        gems_hub.process_data(start_date=start_date, end_date=end_date)


def main_streaming_backfill():
    """大量回填建議用 run_pipeline：逐檔「下載 → 網格化 → 刪原始」，磁碟用量持平。

    extract_bbox 以 getExtractFileItem.do 做 server-side 區域裁切
    （每檔 ~270 MB → ~2-3 MB），並可用 max_workers 併發下載。
    """
    gems_hub = GEMSHub()
    gems_hub.run_pipeline(
        product_type='NO2',
        start_date='2022-01-01',
        end_date='2022-12-31',
        ver=None,
        level='L2',
        extract_bbox=(119, 123, 21, 26),  # (lon_min, lon_max, lat_min, lat_max) 台灣
        max_workers=3,
        keep_raw=False,
        skip_existing=True,
    )


if __name__ == "__main__":
    main()
    # 大量回填改用：
    # main_streaming_backfill()
