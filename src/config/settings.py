"""API 設定和常數"""
from pathlib import Path
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# API URLs
COPERNICUS_TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
COPERNICUS_BASE_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1"
COPERNICUS_DOWNLOAD_URL = "https://zipper.dataspace.copernicus.eu/odata/v1/Products"

# HTTP 設定
RETRY_SETTINGS = {
    'total': 5,
    'backoff_factor': 2,
    'status_forcelist': [429, 500, 502, 503, 504]
}

CHUNK_SIZE = 8192
DEFAULT_TIMEOUT = 60
DOWNLOAD_TIMEOUT = 180

# 存儲路徑
BASE_DIR = Path("/Users/chanchihyu/Sentinel_data")
RAW_DATA_DIR = BASE_DIR / "raw"
PROCESSED_DATA_DIR = BASE_DIR / "processed"
FIGURE_DIR = BASE_DIR / "figure"
LOGS_DIR = BASE_DIR / "logs"


def setup_directory_structure(start_date: str, end_date: str):
    """依照開始和結束時間設定資料夾結構"""
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')

    # 遍歷範圍內的所有月份
    current_date = start
    while current_date <= end:
        year = current_date.strftime('%Y')
        month = current_date.strftime('%m')

        # 構建每個月份的 figure、processed 和 raw 路徑
        for base_dir in [FIGURE_DIR, PROCESSED_DATA_DIR, RAW_DATA_DIR]:
            month_dir = base_dir / year / month
            month_dir.mkdir(parents=True, exist_ok=True)

        # 移動到下個月
        if month == "12":
            current_date = current_date.replace(year=current_date.year + 1, month=1, day=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1, day=1)


def ensure_directories():
    """確保所有必要的目錄存在"""
    directories = [RAW_DATA_DIR, PROCESSED_DATA_DIR, FIGURE_DIR, LOGS_DIR]
    for directory in directories:
        try:
            directory.mkdir(parents=True, exist_ok=True)
            logger.info(f"確保目錄存在: {directory}")
        except Exception as e:
            logger.error(f"創建目錄失敗 {directory}: {str(e)}")
            raise


ensure_directories()
