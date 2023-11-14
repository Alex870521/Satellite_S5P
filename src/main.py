"""主程式"""
import logging
from pathlib import Path
from datetime import datetime

from api.sentinel_api import Sentinel5PDataFetcher
from processing.data_processor import NO2Processor
from utils.logger import setup_logging
from config.settings import setup_directory_structure
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm
from rich.align import Align

# 定義常數
PANEL_WIDTH = 130

console = Console(force_terminal=True, color_system="auto", width=130)  # 使用您想要的寬度
logger = logging.getLogger(__name__)


def rich_print(message: str, width: int = PANEL_WIDTH, confirm: bool = False) -> bool | None:
    """統一的訊息顯示函數"""
    if confirm:
        return Confirm.ask(
            f"[bold cyan]{message}[/bold cyan]",  # 在訊息中加入樣式
            default=True,
            show_default=True
        )

    console.print(Panel(
        Align.center(f"[bold cyan]{message}[/bold cyan]"),
        width=width,
        expand=True,
        border_style="bright_blue",
        padding=(0, 1)
    ))


def fetch_data(start_date: str, end_date: str):
    """下載數據的工作流程"""
    setup_logging()

    try:
        fetcher = Sentinel5PDataFetcher(max_workers=3)

        rich_print(f"正在獲取 sentinel-5p 衛星數據 (NO\u2082) from {start_date} to {end_date} ...")
        products = fetcher.fetch_no2_data(
            start_date=start_date,
            end_date=end_date,
            bbox=(118, 20, 124, 27),
            limit=None
        )

        if products:
            if rich_print("是否要下載數據？", confirm=True):
                rich_print(f"開始下載 sentinel-5p 衛星數據 (NO\u2082) from {start_date} to {end_date} ...")
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


def process_data(start_date: str, end_date: str):
    """處理數據的工作流程"""
    setup_logging()

    try:
        processor = NO2Processor(
            interpolation_method='griddata',
            resolution=0.02,
            mask_value=0.5
        )

        # 改用 rich style 的輸入提示
        if rich_print("是否要處理數據？", confirm=True):
            logger.info(f"Start processing data from {start_date} to {end_date}")
            processor.process_each_data(start_date, end_date, use_taiwan_mask=False)
            rich_print("數據完成處理")

    except Exception as e:
        error_message = f"處理數據失敗: {str(e)}"
        rich_print(error_message)
        logger.error(error_message)


if __name__ == "__main__":
    # 設定參數
    start, end = '2024-11-01', '2024-11-10'

    # 設定輸入輸出配置
    setup_directory_structure(start, end)

    # 下載數據
    fetch_data(start, end)

    # 處理數據
    # process_data(start, end)
