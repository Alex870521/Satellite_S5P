"""主程式"""
import logging
import asyncio
from datetime import datetime

from src.api.sentinel_api import S5PFetcher
from src.processing.data_processor import S5Processor

from src.config.richer import rich_print
from src.config.catalog import ClassInput, TypeInput, PRODUCT_CONFIGS
from src.config.setup import setup
from src.config.settings import FILTER_BOUNDARY


logger = logging.getLogger(__name__)


async def fetch_data(file_class: ClassInput,
                     file_type: TypeInput,
                     start_date: str | datetime,
                     end_date: str | datetime):
    """下載數據的工作流程"""
    try:
        rich_print(
            f"Fetching sentinel-5p products ({PRODUCT_CONFIGS[file_type].display_name}) from {start_date} to {end_date} ...")

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
            if rich_print("Do you want to download data？", confirm=True):
                rich_print(
                    f"Start download sentinel-5p products ({PRODUCT_CONFIGS[file_type].display_name}) from {start_date} to {end_date} ...")
                fetcher.parallel_download(products)
                rich_print("Data download completed！")
            else:
                rich_print("Download canceled")
        else:
            rich_print("No data matching the criteria was found")

    except Exception as e:
        error_message = f"Failed to download data: {str(e)}"
        rich_print(error_message)
        logger.error(error_message)


def process_data(file_class: ClassInput,
                 file_type: TypeInput,
                 start_date: str | datetime,
                 end_date: str | datetime):
    """處理數據的工作流程"""
    try:
        if rich_print("Do you want to process the data？", confirm=True):
            rich_print(
                f"Processing sentinel-5p products ({PRODUCT_CONFIGS[file_type].display_name}) from {start_date} to {end_date} ...")

            processor = S5Processor(
                interpolation_method='rbf',
                resolution=(5.5, 3.5),
                mask_qc_value=0.5
            )

            processor.process_each_data(
                file_class=file_class,
                file_type=file_type,
                start_date=start_date,
                end_date=end_date,
            )

            rich_print("Data processing completed")
        else:
            rich_print("Processing canceled")

    except Exception as e:
        error_message = f"Failed to process data: {str(e)}"
        rich_print(error_message)
        logger.error(error_message)


def main():
    # 步驟：
    # 1. 前往src.config.settings中更改輸出路徑（硬碟路徑）
    # 2. 設定參數
    start, end = '2025-03-01', '2025-03-13'
    file_class: ClassInput = 'NRTI'
    file_type: TypeInput = 'NO2___'

    # 3. 設定輸入輸出配置
    setup(file_type=file_type, start_date=start, end_date=end)

    # 4. 下載數據 (需要有.env 內含 COPERNICUS 帳號密碼才能用)
    asyncio.run(fetch_data(file_class=file_class, file_type=file_type, start_date=start, end_date=end))

    # 5. 處理與繪製數據
    process_data(file_class=file_class, file_type=file_type, start_date=start, end_date=end)

    # 6. 動畫
    # animate_data(file_type=file_type, start_date=start, end_date=end)


if __name__ == "__main__":
    main()
