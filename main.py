"""主程式"""
import logging
from datetime import datetime

from src.api.sentinel_api import S5PFetcher
from src.processing.data_processor import S5Processor

from src.config.richer import rich_print
from src.config.catalog import ClassInput, TypeInput, PRODUCT_CONFIGS
from src.config import setup_directory_structure, FILTER_BOUNDARY


logger = logging.getLogger(__name__)


def fetch_data(file_class: ClassInput,
               file_type: TypeInput,
               start_date: str | datetime,
               end_date: str | datetime):
    """下載數據的工作流程"""
    try:
        rich_print(
            f"正在獲取 sentinel-5p 衛星數據 ({PRODUCT_CONFIGS[file_type].display_name}) from {start_date} to {end_date} ...")

        fetcher = S5PFetcher(max_workers=3)

        products = fetcher.fetch_data(
            file_class=file_class,
            file_type=file_type,
            start_date=start_date,
            end_date=end_date,
            boundary=FILTER_BOUNDARY,
            limit=None
        )

        if products:
            if rich_print("是否要下載數據？", confirm=True):
                rich_print(
                    f"開始下載 sentinel-5p 衛星數據 ({PRODUCT_CONFIGS[file_type].display_name}) from {start_date} to {end_date} ...")
                fetcher.parallel_download(products)
                rich_print("數據下載完成！")
            else:
                rich_print("已取消下載操作")
        else:
            rich_print("找不到符合條件的數據")

    except Exception as e:
        error_message = f"下載數據失敗: {str(e)}"
        rich_print(error_message)
        logger.error(error_message)


def process_data(file_class: ClassInput,
                 file_type: TypeInput,
                 start_date: str | datetime,
                 end_date: str | datetime):
    """處理數據的工作流程"""
    try:
        if rich_print("是否要處理數據？", confirm=True):
            rich_print(
                f"正在處理 sentinel-5p 衛星數據 ({PRODUCT_CONFIGS[file_type].display_name}) from {start_date} to {end_date} ...")

            processor = S5Processor(
                interpolation_method='kdtree',
                resolution=0.02,
                mask_qc_value=0.5
            )

            processor.process_each_data(
                file_class=file_class,
                file_type=file_type,
                start_date=start_date,
                end_date=end_date,
            )

            rich_print("數據完成處理")
        else:
            rich_print("已取消處理操作")

    except Exception as e:
        error_message = f"處理數據失敗: {str(e)}"
        rich_print(error_message)
        logger.error(error_message)


def main():
    # 設定參數
    start, end = '2024-03-01', '2024-03-02'
    file_class: ClassInput = 'OFFL'
    file_type: TypeInput = 'NO2___'

    # 設定輸入輸出配置
    setup_directory_structure(file_type=file_type, start_date=start, end_date=end)

    # 下載數據
    # fetch_data(file_class=file_class, file_type=file_type, start_date=start, end_date=end)

    # 處理與繪製數據
    process_data(file_class=file_class, file_type=file_type, start_date=start, end_date=end)

    # 動畫
    # animate_data(file_type=file_type, start_date=start, end_date=end)


if __name__ == "__main__":
    main()