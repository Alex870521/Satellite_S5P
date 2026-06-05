"""
衛星數據處理Pipeline Controller
採用物件導向設計，提供更好的錯誤處理和資源管理
"""
import os
import logging
import asyncio
import threading
import concurrent.futures
from datetime import datetime, timedelta
from pathlib import Path
import schedule

from src.api import SENTINEL5PHub, MODISHub, ERA5Hub, GEMSHub
from src.config.settings import FILTER_BOUNDARY, FIGURE_BOUNDARY, DATA_RETENTION_DAYS, BASE_DIR, ERA5_STATIONS, REGIONS
from src.config.credentials import check_credentials
from src.utils.file_retention_manager import FileRetentionManager


class SatelliteDataController:
    """衛星數據處理控制器"""

    def __init__(self, data_root: Path, region: str | None = None):
        self.logger = self._setup_logging()
        self.data_root = data_root

        # 處理區域：CLI --region > env SATELLITE_REGION > 'taiwan'。
        # 非 taiwan 會輸出到 processed_<region>/，web 端自動偵測。
        region = (region or os.getenv('SATELLITE_REGION', 'taiwan')).lower()
        if region not in REGIONS:
            raise ValueError(f"未知區域 '{region}'，可用: {sorted(REGIONS)}")
        self.region = region
        self.region_bounds = REGIONS[region]
        self.logger.info(f"Pipeline 區域: {region} bounds={self.region_bounds}")

        # 創建線程池執行器
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

        # ERA5 配置
        self.era5_stations = ERA5_STATIONS
        self.era5_boundary = FIGURE_BOUNDARY

        # 追蹤正在處理的檔案，防止清理時刪除
        self._processing_lock = threading.Lock()
        self._processing_dirs: set[str] = set()

    def _setup_logging(self):
        """設置日誌配置"""
        log_dir = BASE_DIR / 'logs'
        log_dir.mkdir(exist_ok=True)

        log_file = log_dir / f'satellite_pipeline_{datetime.now().strftime("%Y-%m-%d")}.log'
        log_format = '%(asctime)s - [%(name)s] - %(levelname)s - %(message)s'

        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )

        return logging.getLogger(self.__class__.__name__)

    def get_current_half_year_period(self):
        """獲取當前日期對應的半年期間"""
        current_date = datetime.now()
        year = current_date.year

        if current_date.month <= 6:
            start_date = datetime(year, 1, 1)
            end_date = datetime(year, 6, 30)
        else:
            start_date = datetime(year, 7, 1)
            end_date = datetime(year, 12, 31)

        return start_date, end_date, f"{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}"

    def should_update_era5_data(self):
        """判斷是否需要更新 ERA5 數據（檔案不存在或已過期才更新）"""
        start_date, _, current_period = self.get_current_half_year_period()
        era5_dir = BASE_DIR / "ERA5"

        if era5_dir.exists():
            for file in era5_dir.iterdir():
                if file.is_file() and current_period in file.name:
                    # 檔案存在，檢查是否需要增量更新（每週更新一次以補齊新資料）
                    file_mtime = datetime.fromtimestamp(file.stat().st_mtime)
                    days_since_update = (datetime.now() - file_mtime).days
                    if days_since_update < 7:
                        self.logger.info(f"ERA5 文件 {file.name} 於 {days_since_update} 天前更新，跳過")
                        return False
                    self.logger.info(f"ERA5 文件 {file.name} 已超過 7 天未更新，將進行更新")
                    return True

        self.logger.info(f"未找到當前期間 ({current_period}) 的 ERA5 文件，將創建新文件")
        return True

    def _mark_processing(self, data_type: str):
        """標記某資料類型正在處理中"""
        with self._processing_lock:
            self._processing_dirs.add(data_type)

    def _unmark_processing(self, data_type: str):
        """取消標記某資料類型的處理狀態"""
        with self._processing_lock:
            self._processing_dirs.discard(data_type)

    def _is_processing(self, data_type: str) -> bool:
        """檢查某資料類型是否正在處理中"""
        with self._processing_lock:
            return data_type in self._processing_dirs

    async def daily_satellite_task(self):
        """每日衛星數據處理任務（各資料源獨立執行，互不影響）"""
        self.logger.info("開始執行每日衛星數據處理任務")

        today = datetime.now()
        thirty_days_ago = today - timedelta(days=30)
        results = {}

        # 處理 Sentinel-5P（失敗不影響 MODIS）
        try:
            await self._process_sentinel5p(thirty_days_ago, today)
            results['sentinel5p'] = 'success'
        except Exception as e:
            self.logger.error(f"Sentinel-5P 處理失敗: {str(e)}")
            results['sentinel5p'] = f'failed: {e}'

        # 處理 MODIS（失敗不影響其他）
        try:
            await self._process_modis(thirty_days_ago, today)
            results['modis'] = 'success'
        except Exception as e:
            self.logger.error(f"MODIS 處理失敗: {str(e)}")
            results['modis'] = f'failed: {e}'

        # 處理 GEMS（失敗不影響其他）
        try:
            await self._process_gems(thirty_days_ago, today)
            results['gems'] = 'success'
        except Exception as e:
            self.logger.error(f"GEMS 處理失敗: {str(e)}")
            results['gems'] = f'failed: {e}'

        # 彙總結果
        failed = [k for k, v in results.items() if v != 'success']
        if failed:
            self.logger.warning(f"每日任務部分失敗: {', '.join(failed)}")
        else:
            self.logger.info("每日衛星數據處理任務全部完成")

    async def _process_sentinel5p(self, start_date, end_date):
        """處理 Sentinel-5P 數據"""
        file_class = 'NRTI'
        # TODO: 暫時只跑 NO2；要恢復完整集合改回 ['NO2___', 'HCHO__', 'CO____']
        file_types = ['NO2___']

        self._mark_processing("Sentinel-5P")
        try:
            for file_type in file_types:
                try:
                    self.logger.info(f"處理 Sentinel-5P {file_type} 數據")

                    sentinel_hub = SENTINEL5PHub(max_workers=3, region=self.region)
                    # taiwan 維持原本的緊框 filter；其他區域用該區域的 bounds
                    boundary = FILTER_BOUNDARY if self.region == 'taiwan' else self.region_bounds
                    products = sentinel_hub.fetch_data(
                        file_class=file_class,
                        file_type=file_type,
                        start_date=start_date,
                        end_date=end_date,
                        boundary=boundary
                    )

                    if products:
                        sentinel_hub.download_data(products)
                        success = sentinel_hub.process_data()

                        if success:
                            self.logger.info(f"Sentinel-5P {file_type} 處理成功")
                        else:
                            self.logger.error(f"Sentinel-5P {file_type} 處理失敗")
                    else:
                        self.logger.info(f"無可用的 Sentinel-5P {file_type} 數據")

                except Exception as e:
                    self.logger.error(f"Sentinel-5P {file_type} 處理出錯: {str(e)}")
                    continue  # 繼續處理下一個類型
        finally:
            self._unmark_processing("Sentinel-5P")

    async def _process_modis(self, start_date, end_date):
        """處理 MODIS 數據"""
        # MODIS 的裁切是 FILTER_BOUNDARY + 寫死的台灣格網，尚未區域化 → 非台灣略過
        if self.region != 'taiwan':
            self.logger.info(f"MODIS 尚未支援區域裁切，region={self.region} 略過")
            return

        file_types = ['MYD04_L2', 'MOD04_L2', 'MCD19A2']

        self._mark_processing("MODIS")
        try:
            for file_type in file_types:
                try:
                    self.logger.info(f"處理 MODIS {file_type} 數據")

                    modis_hub = MODISHub()
                    products = modis_hub.fetch_data(
                        file_type=file_type,
                        start_date=start_date,
                        end_date=end_date
                    )

                    if products:
                        modis_hub.download_data(products)
                        success = modis_hub.process_data()

                        if success:
                            self.logger.info(f"MODIS {file_type} 處理成功")
                        else:
                            self.logger.error(f"MODIS {file_type} 處理失敗")
                    else:
                        self.logger.info(f"無可用的 MODIS {file_type} 數據")

                except Exception as e:
                    self.logger.error(f"MODIS {file_type} 處理出錯: {str(e)}")
                    continue  # 繼續處理下一個類型
        finally:
            self._unmark_processing("MODIS")

    async def _process_gems(self, start_date, end_date):
        """處理 GEMS 數據（streaming run_pipeline：逐檔下載→網格化→刪原始）。

        用 server 端區域裁切（extract_bbox=台灣）把每檔降到數 MB，`skip_existing`
        可斷點續傳 → 每天只補新時次。`keep_raw=False` 只刪「本次剛下載的小原始檔」，
        不動既有 processed/figure 歷史（GEMS 歷史 archive 不在清理範圍內）。
        """
        product_types = ['NO2']  # 目前只跑 NO2（與既有 archive 一致）

        self._mark_processing("GEMS")
        try:
            for product_type in product_types:
                try:
                    self.logger.info(f"處理 GEMS {product_type} 數據")

                    gems_hub = GEMSHub(region=self.region)
                    # taiwan 維持原本 FIGURE_BOUNDARY 的 server 端裁切；其他區域用該區域 bounds
                    extract_bbox = FIGURE_BOUNDARY if self.region == 'taiwan' else self.region_bounds
                    stats = gems_hub.run_pipeline(
                        product_type=product_type,
                        start_date=start_date,
                        end_date=end_date,
                        extract_bbox=extract_bbox,     # (lon_min,lon_max,lat_min,lat_max)
                        max_workers=3,                 # 對政府 API 保守
                        skip_existing=True,            # 可續傳，只補新時次
                        keep_raw=False,                # 網格化後刪本次下載的小原始檔
                        make_figures=True,
                    )
                    self.logger.info(f"GEMS {product_type} 完成: {stats}")

                except Exception as e:
                    self.logger.error(f"GEMS {product_type} 處理出錯: {str(e)}")
                    continue  # 繼續處理下一個類型
        finally:
            self._unmark_processing("GEMS")

    async def monthly_era5_task(self):
        """每月 ERA5 邊界層高度數據處理任務"""
        self.logger.info("開始執行每月 ERA5 邊界層高度數據處理任務")

        try:
            if not self.should_update_era5_data():
                self.logger.info("當前期間的 ERA5 數據已是最新，跳過更新")
                return

            start_date, end_date, period_name = self.get_current_half_year_period()

            self.logger.info(f"正在處理期間: {period_name}")

            era5_hub = ERA5Hub(timezone='Asia/Taipei')

            # 獲取數據
            era5_hub.fetch_data(
                start_date=start_date,
                end_date=end_date,
                boundary=self.era5_boundary,
                variables=['boundary_layer_height'],
                pressure_levels=None,
                download_mode='all_at_once'
            )

            # 下載數據
            era5_hub.download_data()

            # 處理數據
            era5_hub.process_data(stations=self.era5_stations)

            self.logger.info(f"ERA5 邊界層高度數據處理完成 - 期間: {period_name}")

        except Exception as e:
            self.logger.error(f"ERA5 數據處理失敗: {str(e)}")
            raise

    def run_era5_task(self):
        """每週執行 ERA5 數據處理任務"""
        current_date = datetime.now()
        self.logger.info(f"每週 ERA5 任務執行 ({current_date.strftime('%Y-%m-%d %H:%M')})")
        asyncio.run(self.monthly_era5_task())

    def clean_data_task(self):
        """數據清理任務（跳過正在處理中的資料類型）"""
        self.logger.info("開始執行週期性檔案清理...")

        try:
            cleaner = FileRetentionManager(retention_days=DATA_RETENTION_DAYS)

            # 清理 Sentinel-5P 數據（跳過處理中）
            if self._is_processing("Sentinel-5P"):
                self.logger.info("Sentinel-5P 正在處理中，跳過清理")
            else:
                self._clean_satellite_data(cleaner, "Sentinel-5P", [".png", ".nc", ".tiff"])

            # 清理 MODIS 數據（跳過處理中）
            if self._is_processing("MODIS"):
                self.logger.info("MODIS 正在處理中，跳過清理")
            else:
                self._clean_satellite_data(cleaner, "MODIS", [".png", ".hdf"])

            # 清理 GEMS 數據（跳過處理中）。run_pipeline 已刪本次原始檔，
            # 這裡按 retention 清理超過保留天數的 processed/figure 輸出。
            if self._is_processing("GEMS"):
                self.logger.info("GEMS 正在處理中，跳過清理")
            else:
                self._clean_satellite_data(cleaner, "GEMS", [".png", ".nc"])

            # ERA5 數據不清理
            self.logger.info("ERA5 數據跳過清理（保留所有歷史數據）")

            self.logger.info("所有數據清理完成")

        except Exception as e:
            self.logger.error(f"數據清理任務失敗: {str(e)}")

    def _clean_satellite_data(self, cleaner, data_type, extensions):
        """清理指定類型的衛星數據"""
        data_dir = BASE_DIR / data_type
        if data_dir.exists():
            results = cleaner.clean_satellite_data(data_dir, file_extensions=extensions)
            cleaned_count = sum(
                r.get('cleaned_files', 0) for r in results.values()
                if isinstance(r, dict)
            )
            self.logger.info(f"{data_type} 清理完成: {cleaned_count} 檔案")

    def run_main_pipeline(self):
        """運行主要的數據處理流程"""
        self.logger.info("開始執行主要數據處理流程")

        try:
            # 執行每日衛星數據任務
            asyncio.run(self.daily_satellite_task())
            self.logger.info("主要數據處理流程完成")

        except Exception as e:
            self.logger.error(f"主要數據處理流程失敗: {str(e)}")

    def _run_scheduler(self):
        """在獨立線程中運行排程器"""
        while not self._stop_event.is_set():
            schedule.run_pending()
            self._stop_event.wait(timeout=60)  # 每 60 秒檢查一次，可被中斷

    def start_pipeline(self):
        """啟動數據處理流程"""
        self.logger.info("啟動衛星數據自動處理服務")

        # 啟動前驗證所有 API 憑證
        report = check_credentials(health_check=True)
        if not report.all_ok:
            self.logger.error("API 憑證驗證失敗，請修正後再啟動 pipeline")
            return

        self._stop_event = threading.Event()

        # 清除現有排程
        schedule.clear()

        # 設定排程任務
        schedule.every().day.at("08:00").do(self.run_main_pipeline)
        schedule.every().sunday.at("02:00").do(self.run_era5_task)
        schedule.every().sunday.at("01:00").do(self.clean_data_task)

        self.logger.info("排程已設定: 每日 08:00 主管線 | 週日 02:00 ERA5 | 週日 01:00 清理")

        # 系統啟動時立即執行
        try:
            self.logger.info("系統啟動，立即執行一次 ERA5 數據處理")
            asyncio.run(self.monthly_era5_task())
        except Exception as e:
            self.logger.error(f"初始 ERA5 執行失敗: {str(e)}")

        # 在獨立線程中運行排程器，主線程保持回應
        scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
        scheduler_thread.start()
        self.logger.info("排程器已在背景線程啟動（每 60 秒檢查一次）")

        try:
            scheduler_thread.join()  # 等待排程線程（直到 stop_event 被設定）
        except KeyboardInterrupt:
            self.logger.info("收到停止訊號，正在關閉服務...")
            self._stop_event.set()
            scheduler_thread.join(timeout=10)
            self.logger.info("服務已停止")
        finally:
            self._cleanup()

    def _cleanup(self):
        """清理資源"""
        if hasattr(self, '_stop_event'):
            self._stop_event.set()
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=True, cancel_futures=True)

    def __del__(self):
        """清理資源"""
        self._cleanup()


def main():
    """主函數"""
    import argparse
    parser = argparse.ArgumentParser(description='衛星數據處理 Pipeline')
    parser.add_argument(
        '--region', default=None,
        help=f"處理區域 {sorted(REGIONS)}；預設讀 env SATELLITE_REGION,再退回 taiwan",
    )
    args = parser.parse_args()

    try:
        controller = SatelliteDataController(BASE_DIR, region=args.region)
        controller.start_pipeline()

    except Exception as e:
        logging.error(f"Pipeline 啟動失敗: {str(e)}")
        raise


if __name__ == "__main__":
    main()
