from pathlib import Path
from datetime import datetime
import logging
from dateutil.relativedelta import relativedelta

from src.config.settings import RAW_DATA_DIR, PROCESSED_DATA_DIR, GEOTIFF_DIR, FIGURE_DIR, LOGS_DIR
from src.config.catalog import TypeInput


def setup_logging():
    """設置日誌配置"""
    log_file = LOGS_DIR / f"Satellite_S5P_{datetime.now().strftime('%Y%m')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name).10s - %(message)s - [%(lineno)d]',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )


def create_date_folders(base_dir: Path, file_type: str, start: datetime, end: datetime) -> None:
    """創建指定日期範圍的資料夾結構"""
    current = start
    while current <= end:
        year, month = current.strftime('%Y'), current.strftime('%m')
        (base_dir / file_type / year / month).mkdir(parents=True, exist_ok=True)
        current += relativedelta(months=1)


def setup(file_type: TypeInput,
          start_date: str | datetime,
          end_date: str | datetime) -> None:
    """設置整個專案環境

    Args:
        file_type: 檔案類型
        start_date: 開始日期
        end_date: 結束日期
    """
    try:
        # 1. 建立基本目錄
        for directory in [RAW_DATA_DIR, PROCESSED_DATA_DIR, GEOTIFF_DIR, FIGURE_DIR, LOGS_DIR]:
            directory.mkdir(parents=True, exist_ok=True)

        # 2. 設置日誌
        setup_logging()
        logger = logging.getLogger(__name__)
        logger.info("Setting up project environment...")

        # 3. 解析日期
        start = start_date if isinstance(start_date, datetime) else datetime.strptime(start_date, '%Y-%m-%d')
        end = end_date if isinstance(end_date, datetime) else datetime.strptime(end_date, '%Y-%m-%d')

        # 4. 建立資料目錄結構
        for base_dir in [FIGURE_DIR, PROCESSED_DATA_DIR, GEOTIFF_DIR, RAW_DATA_DIR]:
            create_date_folders(base_dir, file_type, start, end)

        logger.info(f"Setup completed for {file_type} from {start.date()} to {end.date()}")

    except Exception as e:
        logging.error(f"Setup failed: {str(e)}")
        raise


""" I/O structure
Main Folder (Sentinel_data)
├── figure
│   ├── NO2
│   │   ├── 2023
│   │   │   ├── 01
│   │   │   └── ...
│   │   └── 2024
│   │       ├── 01
│   │       └── ...
│   └── ...
├── logs
│   └── Satellite_S5P_202411.log
├── processed
│   ├── NO2
│   │   ├── 2023
│   │   │   ├── 01
│   │   │   └── ...
│   │   └── 2024
│   │       ├── 01
│   │       └── ...
│   └── ...
└── raw
    ├── NO2
    │   ├── 2023
    │   │   ├── 01
    │   │   └── ...
    │   └── 2024
    │       ├── 01
    │       └── ...
    └── ...
"""