"""
檔案保留期限管理系統
用於自動清理超過保留期限的衛星數據檔案
支持嵌套目錄結構: Satellite/figure/file_type/年份/月份/檔案
"""
import logging
from pathlib import Path
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class FileRetentionManager:
    """管理檔案保留期限，自動清理過期檔案"""

    def __init__(self, retention_days):
        """
        初始化檔案保留管理器

        參數:
        retention_days (int): 要保留檔案的天數
        """
        self.retention_days = retention_days

    def clean_directories(self, base_dir, subdirs=None):
        """
        清理特定目錄下超過保留期限的檔案

        參數:
        base_dir (str or Path): 基礎目錄路徑
        subdirs (list): 子目錄列表，如果為None則直接清理base_dir

        返回:
        int: 被清理的檔案數量
        """
        base_path = Path(base_dir)

        if not base_path.exists():
            logger.warning(f"目錄不存在: {base_path}")
            return 0

        dirs_to_clean = []
        if subdirs:
            for subdir in subdirs:
                full_path = base_path / subdir
                if full_path.exists():
                    dirs_to_clean.append(full_path)
        else:
            dirs_to_clean.append(base_path)

        total_removed = 0
        cutoff_date = datetime.now() - timedelta(days=self.retention_days)

        for directory in dirs_to_clean:
            removed = self._clean_directory(directory, cutoff_date)
            total_removed += removed

        return total_removed

    def _clean_directory(self, directory, cutoff_date):
        """
        清理單個目錄中的舊檔案

        參數:
        directory (Path): 目錄路徑
        cutoff_date (datetime): 截止日期，早於此日期的檔案將被刪除

        返回:
        int: 被刪除的檔案數量
        """
        logger.info(f"開始清理目錄: {directory}")
        removed_count = 0

        # 獲取目錄中所有檔案
        files = [f for f in directory.iterdir() if f.is_file()]

        for file_path in files:
            # 獲取檔案修改時間
            file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)

            # 如果檔案早於截止日期，則刪除
            if file_mtime < cutoff_date:
                try:
                    file_path.unlink()
                    logger.info(f"已刪除舊檔案: {file_path}")
                    removed_count += 1
                except Exception as e:
                    logger.error(f"刪除檔案 {file_path} 時出錯: {str(e)}")

        return removed_count

    def clean_satellite_figure_data(self, data_root, file_types=None):
        """
        清理衛星圖像目錄，支持嵌套目錄結構: Satellite/figure/file_type/年份/月份/檔案

        參數:
        data_root (str or Path): 數據根目錄 (通常是 Config.DATA_ROOT)
        file_types (list): 檔案類型列表，如 ['NO2____', 'CO_____']，如果為None則清理所有類型

        返回:
        dict: 每個類型清理的檔案數量
        """
        data_root_path = Path(data_root)
        figure_path = data_root_path / "figure"

        if not figure_path.exists():
            logger.warning(f"衛星圖像目錄不存在: {figure_path}")
            return {}

        results = {}

        # 如果未指定file_types，則獲取所有子目錄作為file_types
        if file_types is None:
            file_types = [d.name for d in figure_path.iterdir() if d.is_dir()]

        cutoff_date = datetime.now() - timedelta(days=self.retention_days)

        # 遍歷每個文件類型目錄
        for file_type in file_types:
            file_type_dir = figure_path / file_type
            if not file_type_dir.exists():
                logger.warning(f"文件類型目錄不存在: {file_type_dir}")
                results[file_type] = 0
                continue

            removed_count = 0

            # 遍歷年份目錄
            for year_dir in [d for d in file_type_dir.iterdir() if d.is_dir()]:
                # 遍歷月份目錄
                for month_dir in [d for d in year_dir.iterdir() if d.is_dir()]:
                    # 清理所有PNG檔案
                    png_files = list(month_dir.glob("*.png"))

                    for png_file in png_files:
                        # 獲取檔案修改時間
                        file_mtime = datetime.fromtimestamp(png_file.stat().st_mtime)

                        # 如果檔案早於截止日期，則刪除
                        if file_mtime < cutoff_date:
                            try:
                                png_file.unlink()
                                logger.info(f"已刪除舊圖像檔案: {png_file}")
                                removed_count += 1
                            except Exception as e:
                                logger.error(f"刪除檔案 {png_file} 時出錯: {str(e)}")

                    # 如果月份目錄為空，也刪除它
                    if not any(month_dir.iterdir()):
                        try:
                            month_dir.rmdir()
                            logger.info(f"已刪除空月份目錄: {month_dir}")
                        except Exception as e:
                            logger.error(f"刪除目錄 {month_dir} 時出錯: {str(e)}")

                # 如果年份目錄為空，也刪除它
                if not any(year_dir.iterdir()):
                    try:
                        year_dir.rmdir()
                        logger.info(f"已刪除空年份目錄: {year_dir}")
                    except Exception as e:
                        logger.error(f"刪除目錄 {year_dir} 時出錯: {str(e)}")

            results[file_type] = removed_count

        return results

    def clean_all_satellite_data(self, data_root, file_types=None):
        """
        清理所有衛星數據相關檔案

        參數:
        data_root (str or Path): 數據根目錄 (通常是 Config.DATA_ROOT)
        file_types (list): 檔案類型列表，如 ['NO2____', 'CO_____']

        返回:
        dict: 各類別被清理的檔案數量
        """
        data_root_path = Path(data_root)
        results = {}

        # 清理圖像檔案
        figure_results = self.clean_satellite_figure_data(data_root_path, file_types)
        results.update({f'figure_{k}': v for k, v in figure_results.items()})

        # 清理下載的原始數據文件 (如果有)
        data_dir = data_root_path / "Satellite" / "data"
        if data_dir.exists():
            download_count = self.clean_directories(data_dir)
            results['data_files'] = download_count

        # 清理處理後的數據文件 (如果有)
        processed_dir = data_root_path / "Satellite" / "processed"
        if processed_dir.exists():
            processed_count = self.clean_directories(processed_dir)
            results['processed_files'] = processed_count

        # 清理標記檔案 (processed_*.flag)
        flag_dir = data_root_path / "Satellite"
        if flag_dir.exists():
            flag_count = self._clean_flag_files(flag_dir,
                                              cutoff_date=datetime.now() - timedelta(days=self.retention_days))
            results['flag_files'] = flag_count

        return results

    def _clean_flag_files(self, directory, cutoff_date):
        """
        清理舊的標記檔案

        參數:
        directory (Path): 目錄路徑
        cutoff_date (datetime): 截止日期

        返回:
        int: 被刪除的檔案數量
        """
        directory_path = Path(directory)
        flag_pattern = "processed_*.flag"
        flag_files = list(directory_path.glob(flag_pattern))

        removed_count = 0

        for flag_file in flag_files:
            # 從檔案名中提取日期
            try:
                file_name = flag_file.name
                date_str = file_name.replace("processed_", "").replace(".flag", "")
                file_date = datetime.strptime(date_str, "%Y-%m-%d")

                if file_date < cutoff_date:
                    flag_file.unlink()
                    logger.info(f"已刪除舊標記檔案: {flag_file}")
                    removed_count += 1
            except Exception as e:
                logger.error(f"處理標記檔案 {flag_file} 時出錯: {str(e)}")

        return removed_count