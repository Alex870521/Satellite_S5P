"""
檔案保留期限管理系統
用於自動清理超過保留期限的衛星數據檔案
"""
import logging
import os
from pathlib import Path
from datetime import datetime, timedelta
import fnmatch

from src.config.settings import DATA_RETENTION_DAYS


logger = logging.getLogger(__name__)


class FileRetentionManager:
    """管理檔案保留期限，自動清理過期檔案"""

    def __init__(self, retention_days=DATA_RETENTION_DAYS):
        """
        初始化檔案清理器

        參數:
            retention_days (int): 檔案保留天數
        """
        self.retention_days = retention_days
        self.cutoff_date = datetime.now() - timedelta(days=retention_days)

    def clean_old_files(self, directory, patterns=None, recursive=True, remove_empty_dirs=True):
        """
        清理指定目錄中的過期檔案

        參數:
            directory (str or Path): 要清理的目錄
            patterns (list): 檔案模式列表，例如 ['*.png', '*.nc']，None表示所有檔案
            recursive (bool): 是否遞歸處理子目錄
            remove_empty_dirs (bool): 是否刪除空目錄

        返回:
            dict: 清理結果統計
        """
        directory = Path(directory)
        if not directory.exists():
            logger.warning(f"目錄不存在: {directory}")
            return {"error": "directory_not_found", "cleaned_files": 0, "removed_dirs": 0}

        stats = {"cleaned_files": 0, "removed_dirs": 0}

        # 清理檔案的函數
        def _clean_dir(current_dir):
            nonlocal stats

            # 先處理檔案
            for item in current_dir.iterdir():
                if item.is_file():
                    # 檢查檔案是否符合模式
                    if patterns:
                        if not any(fnmatch.fnmatch(item.name, p) for p in patterns):
                            continue

                    # 檢查檔案修改時間
                    mtime = datetime.fromtimestamp(item.stat().st_mtime)
                    if mtime < self.cutoff_date:
                        try:
                            item.unlink()
                            logger.info(f"已刪除過期檔案: {item}")
                            stats["cleaned_files"] += 1
                        except Exception as e:
                            logger.error(f"刪除檔案失敗 {item}: {e}")

        # 遞歸處理目錄
        if recursive:
            # 使用os.walk從底層目錄開始處理，這樣可以正確處理空目錄
            for root, dirs, files in os.walk(directory, topdown=False):
                current_dir = Path(root)

                # 清理當前目錄中的檔案
                _clean_dir(current_dir)

                # 檢查目錄是否為空並刪除
                if remove_empty_dirs and not any(current_dir.iterdir()):
                    # 不刪除根目錄
                    if current_dir != directory:
                        try:
                            current_dir.rmdir()
                            logger.info(f"已刪除空目錄: {current_dir}")
                            stats["removed_dirs"] += 1
                        except Exception as e:
                            logger.error(f"刪除空目錄失敗 {current_dir}: {e}")
        else:
            # 僅處理指定目錄（不遞歸）
            _clean_dir(directory)

        return stats

    def clean_satellite_data(self, base_dir, data_types=None, file_extensions=None):
        """
        清理衛星數據專用方法

        參數:
            base_dir (str or Path): 基礎目錄
            data_types (list): 數據類型列表，如 ['figure', 'raw', 'processed']
            file_extensions (list): 檔案副檔名列表，如 ['.png', '.nc', '.hdf']

        返回:
            dict: 清理結果統計
        """
        base_dir = Path(base_dir)
        results = {}

        # 默認清理所有子目錄
        if data_types is None:
            data_types = [d.name for d in base_dir.iterdir() if d.is_dir()]

        # 根據檔案副檔名生成模式
        patterns = None
        if file_extensions:
            patterns = [f'*{ext}' for ext in file_extensions]

        # 按數據類型清理
        for data_type in data_types:
            type_dir = base_dir / data_type
            if type_dir.exists():
                logger.info(f"開始清理 {data_type} 數據...")
                result = self.clean_old_files(type_dir, patterns)
                results[data_type] = result
                logger.info(f"已完成 {data_type} 數據清理: 刪除 {result['cleaned_files']} 檔案, "
                            f"{result['removed_dirs']} 目錄")

        # 總結結果
        total_files = sum(r["cleaned_files"] for r in results.values())
        total_dirs = sum(r["removed_dirs"] for r in results.values())

        logger.info(f"清理完成，共刪除 {total_files} 個過期檔案和 {total_dirs} 個空目錄")
        results["total"] = {"cleaned_files": total_files, "removed_dirs": total_dirs}

        return results

    def estimate_space_savings(self, directory, patterns=None, recursive=True):
        """
        估算清理將節省的空間

        參數:
            directory (str or Path): 要分析的目錄
            patterns (list): 檔案模式列表
            recursive (bool): 是否遞歸處理子目錄

        返回:
            dict: 空間節省統計
        """
        directory = Path(directory)
        stats = {"file_count": 0, "total_bytes": 0, "oldest_file": None, "newest_file": None}

        if not directory.exists():
            return stats

        # 處理目錄的函數
        def _process_dir(current_dir):
            for item in current_dir.iterdir():
                if item.is_file():
                    # 檢查檔案是否符合模式
                    if patterns:
                        if not any(fnmatch.fnmatch(item.name, p) for p in patterns):
                            continue

                    # 檢查檔案修改時間
                    mtime = datetime.fromtimestamp(item.stat().st_mtime)
                    if mtime < self.cutoff_date:
                        file_size = item.stat().st_size
                        stats["file_count"] += 1
                        stats["total_bytes"] += file_size

                        # 更新最舊和最新的過期檔案
                        if stats["oldest_file"] is None or mtime < stats["oldest_file"][1]:
                            stats["oldest_file"] = (str(item), mtime)
                        if stats["newest_file"] is None or mtime > stats["newest_file"][1]:
                            stats["newest_file"] = (str(item), mtime)

                elif item.is_dir() and recursive:
                    _process_dir(item)

        _process_dir(directory)

        # 轉換為易讀格式
        if stats["total_bytes"] < 1024:
            stats["readable_size"] = f"{stats['total_bytes']} B"
        elif stats["total_bytes"] < 1024 * 1024:
            stats["readable_size"] = f"{stats['total_bytes'] / 1024:.2f} KB"
        elif stats["total_bytes"] < 1024 * 1024 * 1024:
            stats["readable_size"] = f"{stats['total_bytes'] / (1024 * 1024):.2f} MB"
        else:
            stats["readable_size"] = f"{stats['total_bytes'] / (1024 * 1024 * 1024):.2f} GB"

        # 修改日期格式
        if stats["oldest_file"]:
            stats["oldest_file"] = {
                "path": stats["oldest_file"][0],
                "date": stats["oldest_file"][1].strftime("%Y-%m-%d %H:%M:%S")
            }
        if stats["newest_file"]:
            stats["newest_file"] = {
                "path": stats["newest_file"][0],
                "date": stats["newest_file"][1].strftime("%Y-%m-%d %H:%M:%S")
            }

        return stats


# 使用範例
if __name__ == "__main__":
    # 設置日誌
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # 初始化清理器，設定30天保留期限
    cleaner = FileRetentionManager(retention_days=30)

    # 估算空間節省
    savings = cleaner.estimate_space_savings("/path/to/data", patterns=["*.png", "*.nc"])
    print(f"預計可釋放 {savings['readable_size']} 的空間，共 {savings['file_count']} 個檔案")

    # 執行清理
    results = cleaner.clean_satellite_data("/path/to/data",
                                           data_types=["figure", "processed"],
                                           file_extensions=[".png", ".nc", ".hdf"])
    print(f"清理完成: {results['total']['cleaned_files']} 檔案已刪除")