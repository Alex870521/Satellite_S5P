"""
自動衛星數據處理Pipeline
每天早上8點自動獲取、處理並繪製當天的衛星數據
包含檔案自動清理功能，避免檔案不斷累積
"""
import os
import logging
import asyncio
import time
from datetime import datetime, timedelta
import schedule
from src.api import SENTINEL5PHub, MODISHub
from src.config.settings import FILTER_BOUNDARY, DATA_RETENTION_DAYS, BASE_DIR
from src.utils.file_retention_manager import FileRetentionManager


# 配置日誌
def setup_logging():
    """設置日誌配置"""
    # 創建日誌目錄
    log_dir = os.path.join(BASE_DIR, 'logs')
    os.makedirs(log_dir, exist_ok=True)

    # 配置根日誌器
    log_file = os.path.join(log_dir, f'satellite_pipeline_{datetime.now().strftime("%Y-%m-%d")}.log')

    # 改進的日誌格式，加入模組名稱
    log_format = '%(asctime)s - [%(name)s] - %(levelname)s - %(message)s'

    # 配置根日誌器
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            # 文件處理器 - 保存到文件
            logging.FileHandler(log_file),
            # 控制台處理器 - 輸出到控制台
            logging.StreamHandler()
        ]
    )

    return logging.getLogger(__name__)

# 初始化日誌
logger = setup_logging()


async def daily_task():
    """每日執行的任務"""
    logger.info("開始執行每日衛星數據處理任務")

    # 設定參數 - 只處理當天的數據
    today = datetime.now()
    seven_days_ago = datetime.now() - timedelta(days=7)

    # 開始執行 Sentinel-5P
    file_class: str = 'NRTI'
    file_type: list[str] = ['NO2___', 'HCHO__', 'CO____']

    for file_tp in file_type:
        # 檢查並下載當天的數據

        sentinel_hub = SENTINEL5PHub(max_workers=3)
        products = sentinel_hub.fetch_data(file_class=file_class,
                                           file_type=file_tp,
                                           start_date=seven_days_ago,
                                           end_date=today,
                                           boundary=FILTER_BOUNDARY)

        if products:
            sentinel_hub.download_data(products)
            success = sentinel_hub.process_data()
            if success:
                logger.info(f"每日衛星數據處理pipeline執行完成 - {today}")
            else:
                logger.error(f"處理數據失敗 - {today}")
        else:
            logger.info(f"今日({today})無可用的衛星數據")

    # 開始執行 MODIS
    file_type: list[str] = ['MYD04', 'MOD04']

    for file_tp in file_type:

        # 檢查並下載當天的數據
        modis_hub = MODISHub()
        products = modis_hub.fetch_data(file_type=file_tp, start_date=seven_days_ago, end_date=today)

        # 如果有數據，則處理並繪製
        if products:
            modis_hub.download_data(products)
            success = modis_hub.process_data()
            if success:
                logger.info(f"每日衛星數據處理pipeline執行完成 - {today}")
            else:
                logger.error(f"處理數據失敗 - {today}")
        else:
            logger.info(f"今日({today})無可用的衛星數據")

def clean_all_data():
    """清理所有衛星數據"""
    logger.info("開始執行週期性檔案清理...")

    cleaner = FileRetentionManager(retention_days=DATA_RETENTION_DAYS)

    # 清理 Sentinel-5P 數據
    sentinel_dir = BASE_DIR / "Sentinel-5P"
    if sentinel_dir.exists():
        sentinel_results = cleaner.clean_satellite_data(
            sentinel_dir,
            file_extensions=[".png", ".nc", ".tiff"]
        )
        logger.info(
            f"Sentinel-5P 清理完成: {sum(r.get('cleaned_files', 0) for r in sentinel_results.values() if isinstance(r, dict))} 檔案")

    # 清理 MODIS 數據
    modis_dir = BASE_DIR / "MODIS"
    if modis_dir.exists():
        modis_results = cleaner.clean_satellite_data(
            modis_dir,
            file_extensions=[".png", ".hdf"]
        )
        logger.info(
            f"MODIS 清理完成: {sum(r.get('cleaned_files', 0) for r in modis_results.values() if isinstance(r, dict))} 檔案")

    logger.info("所有數據清理完成")

def schedule_task():
    """設定定時任務"""
    logger.info("啟動衛星數據自動處理服務")

    # 每天早上8點執行數據處理
    schedule.every().day.at("08:00").do(lambda: asyncio.run(daily_task()))

    # 設定每週清理任務（例如每週日清理）
    schedule.every().sunday.at("01:00").do(clean_all_data)

    # 系統啟動時立即執行一次（無條件）
    logger.info("系統啟動，立即執行一次數據處理")
    asyncio.run(daily_task())

    logger.info("系統啟動，立即執行檔案清理")
    clean_all_data()

    while True:
        schedule.run_pending()
        time.sleep(3600)  # 每小時檢查一次是否有待執行的任務


def main():
    """主函數"""
    try:
        # 啟動定時任務
        schedule_task()
    except KeyboardInterrupt:
        logger.info("服務已停止")
    except Exception as e:
        logger.error(f"服務發生錯誤: {str(e)}")


if __name__ == "__main__":
    main()