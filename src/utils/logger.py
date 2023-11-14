"""日誌配置"""
import logging
from datetime import datetime
from pathlib import Path
from src.config.settings import LOGS_DIR


def setup_logging():
    """設置日誌配置"""
    # 確保日誌目錄存在
    log_dir = Path(LOGS_DIR)
    log_dir.mkdir(parents=True, exist_ok=True)

    # 創建日誌檔案路徑
    log_file = log_dir / f"Satellite_S5p_{datetime.now().strftime('%Y%m')}.log"

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
