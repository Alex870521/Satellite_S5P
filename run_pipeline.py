"""
自動衛星數據處理Pipeline
每天早上8點自動獲取、處理並繪製當天的衛星數據
包含檔案自動清理功能，避免檔案不斷累積
"""
import logging
import asyncio
import time
from datetime import datetime, timedelta
import schedule

from src.api.sentinel_api import S5PFetcher
from src.processing.data_processor import S5Processor
from src.config.catalog import ClassInput, TypeInput, PRODUCT_CONFIGS
from src.config.setup import setup, setup_nasa
from src.config.settings import FILTER_BOUNDARY, DATA_RETENTION_DAYS, LOGS_DIR, BASE_DIR

from main_earthdata import fetch_data, process_data

# 導入檔案保留管理器
from file_retention_manager import FileRetentionManager


# 設定參數 - 只處理當天的數據
today = datetime.now().strftime('%Y-%m-%d')

# 設定輸入輸出配置
setup(file_type='NO2___', start_date=today, end_date=today)


logger = logging.getLogger(__name__)


async def fetch_data_auto(file_class: ClassInput,
                          file_type: TypeInput,
                          start_date: str | datetime,
                          end_date: str | datetime) -> bool:
    """自動下載數據的工作流程，返回是否成功下載"""
    try:
        logger.info(
            f"正在獲取 sentinel-5p 衛星數據 ({PRODUCT_CONFIGS[file_type].display_name}) from {start_date} to {end_date}")

        fetcher = S5PFetcher(max_workers=3)

        products = await fetcher.fetch_data(
            file_class=file_class,
            file_type=file_type,
            start_date=start_date,
            end_date=end_date,
            boundary=FILTER_BOUNDARY,
            limit=None
        )

        if products:
            logger.info(f"找到 {len(products)} 個符合條件的數據產品")
            logger.info(f"開始下載 sentinel-5p 衛星數據")
            fetcher.parallel_download(products)
            logger.info("數據下載完成！")
            return True
        else:
            logger.info("找不到符合條件的數據")
            return False

    except Exception as e:
        error_message = f"下載數據失敗: {str(e)}"
        logger.error(error_message)
        return False


def process_data_auto(file_class: ClassInput,
                      file_type: TypeInput,
                      start_date: str | datetime,
                      end_date: str | datetime) -> bool:
    """自動處理數據的工作流程，返回是否成功處理"""
    try:
        logger.info(
            f"正在處理 sentinel-5p 衛星數據 ({PRODUCT_CONFIGS[file_type].display_name}) from {start_date} to {end_date}")

        processor = S5Processor(
            interpolation_method='rbf',
            resolution=(5.5, 3.5),
            mask_qc_value=0.5
        )

        result = processor.process_each_data(
            file_class=file_class,
            file_type=file_type,
            start_date=start_date,
            end_date=end_date,
        )

        if result:
            logger.info("數據處理完成")
            return True
        else:
            logger.info("沒有數據被處理")
            return False

    except Exception as e:
        error_message = f"處理數據失敗: {str(e)}"
        logger.error(error_message)
        return False


def clean_old_files():
    """清理超過保留期限的舊檔案，只處理OUTPUT_DIR中的檔案"""
    try:
        logger.info(f"開始清理超過 {DATA_RETENTION_DAYS} 天的舊檔案")

        # 初始化檔案保留管理器
        retention_manager = FileRetentionManager(retention_days=DATA_RETENTION_DAYS)

        # 定義要清理的檔案類型（這裡可以根據實際情況調整）
        file_types = ['NO2___', 'HCHO__', 'CO____']

        # 執行清理，只針對FIGURE_DIR中的檔案
        results = retention_manager.clean_satellite_figure_data(BASE_DIR, file_types)

        # 輸出清理結果
        total_removed = sum(results.values())
        logger.info(f"檔案清理完成，共刪除 {total_removed} 個過期檔案")

        for category, count in results.items():
            if count > 0:
                logger.info(f"- {category}: 刪除 {count} 個檔案")

        return total_removed
    except Exception as e:
        logger.error(f"檔案清理過程出錯: {str(e)}")
        return 0


async def daily_task():
    """每日執行的任務"""
    logger.info("開始執行每日衛星數據處理任務")

    # 設定參數 - 只處理當天的數據
    today = datetime.now().strftime('%Y-%m-%d')
    two_days_ago = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

    # 開始執行 Sentinel-5P
    file_class: ClassInput = 'NRTI'
    file_type: list[TypeInput] = ['NO2___', 'HCHO__', 'CO____']

    for file_tp in file_type:
        # 設定輸入輸出配置
        setup(file_type=file_tp, start_date=two_days_ago, end_date=today)

        # 檢查並下載當天的數據
        has_data = await fetch_data_auto(file_class=file_class, file_type=file_tp, start_date=two_days_ago, end_date=today)

        # 如果有數據，則處理並繪製
        if has_data:
            success = process_data_auto(file_class=file_class, file_type=file_tp, start_date=two_days_ago, end_date=today)
            if success:
                logger.info(f"每日衛星數據處理pipeline執行完成 - {today}")
            else:
                logger.error(f"處理數據失敗 - {today}")
        else:
            logger.info(f"今日({today})無可用的衛星數據")

    # 開始執行 MODIS
    file_type: list[str] = ['MYD04', 'MOD04']

    for file_tp in file_type:
        # 設定輸入輸出配置
        setup_nasa(file_type=file_tp, start_date=seven_days_ago, end_date=today)

        # 檢查並下載當天的數據
        has_data = await fetch_data(file_type=file_tp, start_date=seven_days_ago, end_date=today)

        # 如果有數據，則處理並繪製
        if has_data:
            success = process_data(file_type=file_tp, start_date=seven_days_ago, end_date=today)
            if success:
                logger.info(f"每日衛星數據處理pipeline執行完成 - {today}")
            else:
                logger.error(f"處理數據失敗 - {today}")
        else:
            logger.info(f"今日({today})無可用的衛星數據")


    # 執行舊檔案清理任務
    # logger.info("執行舊檔案清理任務")
    # clean_old_files()


def schedule_task():
    """設定定時任務"""
    logger.info("啟動衛星數據自動處理服務")

    # 每天早上8點執行數據處理和檔案清理任務
    schedule.every().day.at("08:00").do(lambda: asyncio.run(daily_task()))

    # 系統啟動時立即執行一次（無條件）
    logger.info("系統啟動，立即執行一次數據處理")
    asyncio.run(daily_task())

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