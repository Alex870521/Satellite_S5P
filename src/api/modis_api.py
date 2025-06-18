import os
import re
from typing import Literal

import earthaccess
from datetime import datetime
from pathlib import Path
from src.config.richer import DisplayManager
from src.api.core import SatelliteHub
from src.processing.modis_processor import MODISProcessor

# 地理範圍設定
SEARCH_BOUNDARY = (119.0, 21.0, 123.0, 26.0)  # 搜索數據的邊界 (west_lon, south_lat, east_lon, north_lat)


class MODISHub(SatelliteHub):
    # API name
    name = 'MODIS'

    def __init__(self):
        super().__init__()
        self._processor = None  # 初始化為 None，延遲創建

    def authentication(self):
        if not os.getenv('EARTHDATA_USERNAME') or not os.getenv('EARTHDATA_PASSWORD'):
            raise EnvironmentError(
                "Missing EARTHDATA credentials. Please set EARTHDATA_USERNAME and EARTHDATA_PASSWORD environment variables"
            )
        return earthaccess.login()

    def fetch_data(self,
                   file_type: str | Literal['MOD04_L2', 'MYD04_L2', 'MOD04_3K', 'MYD04_3K', 'MCD19A1', 'MCD19A2', 'MCD19A3D'],
                   start_date: str | datetime,
                   end_date: str | datetime,
                   boundary: tuple = SEARCH_BOUNDARY
                   ) -> list:
        """ """
        self.file_type = file_type
        self.start_date, self.end_date = self._normalize_time_inputs(start_date, end_date, set_timezone=False)

        # Convert datetime objects to ISO 8601 format strings
        start_date_iso = self.start_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        end_date_iso = self.end_date.strftime("%Y-%m-%dT%H:%M:%S.999Z")

        products = earthaccess.search_data(
            short_name=file_type,
            temporal=(start_date_iso, end_date_iso),
            bounding_box=boundary,
        )

        # 進一步過濾結果，排除 NRT 文件
        filtered_products = []
        for product in products:
            # 檢查是否有下載連結
            if not hasattr(product, 'data_links') or not callable(getattr(product, 'data_links')):
                continue

            links = product.data_links()
            if not links:
                continue

            # 檢查文件名是否包含 .NRT.hdf
            filename = Path(links[0]).name
            if '.NRT.hdf' not in filename:
                filtered_products.append(product)

        # Display product info
        if filtered_products:
            DisplayManager().display_products_nasa(filtered_products)
            return filtered_products
        else:
            self.logger.info("No valid products found (NRT files excluded)")
            return []

    def download_data(self, products):
        if not products:
            self.logger.info("沒有找到符合條件的數據")
            return []

        # 用於跟踪下載的文件
        downloaded_files = []

        # 檢查哪些文件需要下載
        for result in products:
            try:
                # 獲取文件名和下載鏈接
                if not result.data_links():
                    continue

                file_url = result.data_links()[0]
                filename = Path(file_url).name

                # 跳過 NRT 文件
                if '.NRT.hdf' in filename:
                    self.logger.debug(f"跳過 NRT 文件: {filename}")
                    continue

                # 從檔案名稱提取日期信息
                date_match = re.search(r'\.A(\d{7})\.', filename)
                if not date_match:
                    self.logger.error(f"無法從文件名提取日期: {filename}")
                    continue

                date_str = date_match.group(1)
                year = date_str[:4]
                day_of_year = int(date_str[4:7])

                # 將日期轉換為年月
                file_date = datetime.strptime(f"{year}-{day_of_year}", "%Y-%j")
                year_month_dir = file_date.strftime("%Y/%m")

                # 創建目標目錄
                target_dir = self.raw_dir / self.file_type / year_month_dir
                target_dir.mkdir(parents=True, exist_ok=True)

                # 檢查文件是否已存在
                target_file = target_dir / filename
                temp_file = target_dir / f"{filename}.temp"

                if target_file.exists():
                    self.logger.info(f"檔案已存在: {target_file}")
                    downloaded_files.append(str(target_file))
                    continue

                # 下載單個文件
                self.logger.info(f"下載文件: {filename} 到 {target_dir}")

                try:
                    # 直接下載到目標目錄，但使用.temp副檔名
                    downloaded_files_list = earthaccess.download([result], target_dir)
                    # 檢查下載結果
                    if downloaded_files_list and len(downloaded_files_list) > 0:
                        downloaded_path = Path(downloaded_files_list[0])

                        # 如果下載的文件直接是目標格式（.hdf）
                        if downloaded_path.exists():
                            self.logger.info(f"\n成功下載: {downloaded_path}")
                            downloaded_files.append(str(downloaded_path))
                        else:
                            self.logger.error(f"下載失敗: {filename}")
                    else:
                        self.logger.error(f"下載返回空列表: {filename}")
                except Exception as e:
                    self.logger.error(f"下載文件時發生錯誤: {str(e)}")
                    # 清理可能留下的臨時文件
                    if temp_file.exists():
                        try:
                            temp_file.unlink()
                            self.logger.debug(f"刪除臨時文件: {temp_file}")
                        except Exception as cleanup_error:
                            self.logger.error(f"清理臨時文件失敗: {str(cleanup_error)}")

            except Exception as e:
                self.logger.error(f"處理文件時發生錯誤: {str(e)}")

        if not downloaded_files:
            self.logger.info("所有檔案已經存在，無需下載")
        else:
            self.logger.info(f"成功下載 {len(downloaded_files)} 個檔案")

        return downloaded_files

    @property
    def processor(self):
        """延遲創建並返回MODISProcessor實例"""
        if self._processor is None:
            # 確保file_type已被設置
            if not hasattr(self, 'file_type'):
                raise ValueError("未設置file_type，請先呼叫fetch_data方法")

            # 創建處理器實例
            self._processor = MODISProcessor()

            # 設置路徑
            self._processor.raw_dir = self.raw_dir
            self._processor.processed_dir = self.processed_dir
            self._processor.figure_dir = self.figure_dir
            self._processor.logger = self.logger
            self._processor.file_type = self.file_type

        return self._processor

    def process_data(self, pattern=None, start_date=None, end_date=None):
        """處理下載的MODIS數據並生成可視化圖像"""
        # 確保file_type已被設置
        if not hasattr(self, 'file_type'):
            raise ValueError("未設置file_type，請先呼叫fetch_data方法")

        # 如果未指定模式，使用基於file_type的默認模式
        if pattern is None:
            pattern = f"**/{self.file_type}/**/*.hdf"

        # 使用類屬性作為默認日期範圍
        if start_date is None:
            start_date = self.start_date
        if end_date is None:
            end_date = self.end_date

        # 準備日期範圍字符串用於日誌
        if start_date or end_date:
            date_range_str = f"日期範圍: {start_date if start_date else '最早'} 至 {end_date if end_date else '最新'}"
        else:
            date_range_str = "處理所有日期的數據"

        self.logger.info(f"開始處理MODIS數據，模式: {pattern}，{date_range_str}")

        # 使用處理器處理所有文件
        return self.processor.process_all_files(pattern, start_date, end_date)
