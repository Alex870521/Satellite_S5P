import logging
from pathlib import Path
from datetime import datetime

from src.config.settings import RAW_DATA_DIR, PROCESSED_DATA_DIR, FIGURE_DIR, LOGS_DIR, FILTER_BOUNDARY
from src.config.catalog import TypeInput


__all__ = ['setup_directory_structure', 'FILTER_BOUNDARY']


def setup_logging():
    """設置日誌配置"""
    # 確保日誌目錄存在
    log_dir = Path(LOGS_DIR)
    log_dir.mkdir(parents=True, exist_ok=True)

    # 創建日誌檔案路徑
    log_file = log_dir / f"Satellite_S5P_{datetime.now().strftime('%Y%m')}.log"

    # 配置基本設定
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name).10s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()  # 同時輸出到控制台
        ]
    )


def setup_directory_structure(file_type: TypeInput,
                              start_date: str | datetime,
                              end_date: str | datetime):
    """確保所有必要的目錄存在"""
    directories = [RAW_DATA_DIR, PROCESSED_DATA_DIR, FIGURE_DIR, LOGS_DIR]
    for directory in directories:
        try:
            directory.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            raise

    setup_logging()

    """依照開始和結束時間設定資料夾結構"""
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')

    # 遍歷範圍內的所有月份
    current_date = start
    while current_date <= end:
        product_type = file_type
        year = current_date.strftime('%Y')
        month = current_date.strftime('%m')

        # 構建每個月份的 figure、processed 和 raw 路徑
        for base_dir in [FIGURE_DIR, PROCESSED_DATA_DIR, RAW_DATA_DIR]:
            month_dir = base_dir / product_type / year / month
            month_dir.mkdir(parents=True, exist_ok=True)

        # 移動到下個月
        if month == "12":
            current_date = current_date.replace(year=current_date.year + 1, month=1, day=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1, day=1)


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