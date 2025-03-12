"""主程式"""
import logging
import asyncio
from datetime import datetime
from pathlib import Path

from src.api.earthdata_api import EARTHDATAFetcher
from src.processing.modis_processor import MODISProcessor

from src.config.richer import rich_print
from src.config.catalog import ClassInput, TypeInput, PRODUCT_CONFIGS
from src.config.setup import setup, setup_nasa


logger = logging.getLogger(__name__)


async def fetch_data(file_type,
                     start_date: str | datetime,
                     end_date: str | datetime):
    """下載數據的工作流程"""
    try:
        rich_print(
            f"Fetching Earthdata (MODIS) products from {start_date} to {end_date} ...")

        fetcher = EARTHDATAFetcher()

        products = await fetcher.fetch_data(
            file_type=file_type,
            start_date=start_date,
            end_date=end_date,
        )

        if products:
            rich_print(f"Start download Earthdata (MODIS) products from {start_date} to {end_date} ...")
            fetcher.download(products)
            rich_print("Data download completed！")
            return True
        else:
            rich_print("No data matching the criteria was found")

    except Exception as e:
        error_message = f"Failed to download data: {str(e)}"
        rich_print(error_message)
        logger.error(error_message)


def process_data(file_type,
                 start_date: str | datetime,
                 end_date: str | datetime):
    """處理數據的工作流程"""
    try:
        rich_print(
            f"Processing Earthdata (MODIS) products from {start_date} to {end_date} ...")

        processor = MODISProcessor(file_type)

        # 處理所有文件
        processor.process_all_files(
            pattern=f"{file_type}/**/*.hdf",  # 從組織好的文件結構中尋找文件
            start_date=start_date,
            end_date=end_date
        )

        rich_print("Data processing completed")

    except Exception as e:
        error_message = f"Failed to process data: {str(e)}"
        rich_print(error_message)
        logger.error(error_message)


def main():
    # 步驟：
    # 1. 前往src.config.settings中更改輸出路徑（硬碟路徑）
    # 2. 設定參數
    start, end = '2025-03-01', '2025-03-12'
    file_type = "MYD04"

    # 3. 設定輸入輸出配置
    setup_nasa(file_type=file_type, start_date=start, end_date=end)

    # 4. 下載數據 (需要有.env 內含 EARTHDATA_USERNAME and EARTHDATA_PASSWORD 才能用)
    asyncio.run(fetch_data(file_type=file_type, start_date=start, end_date=end))

    # 5. 處理與繪製數據
    process_data(file_type=file_type, start_date=start, end_date=end)


if __name__ == "__main__":
    main()
