import os
import time
import aiohttp  # 異步操作
import requests
import zipfile
import threading
import multiprocessing
from datetime import datetime
from pathlib import Path

from rich.progress import (
    Progress,
    ProgressColumn,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TimeRemainingColumn,
)

from src.api.auth import CopernicusAuth
from src.api.downloader import Downloader
from src.config.settings import (
    COPERNICUS_BASE_URL,
    COPERNICUS_DOWNLOAD_URL,
    DEFAULT_TIMEOUT
)
from src.config.richer import console, DisplayManager
from src.config.catalog import ClassInput, TypeInput, PRODUCT_CONFIGS
from src.api.core import SatelliteHub
from src.processing import SentinelProcessor
from sentinelsat import SentinelAPI


class FileProgressColumn(ProgressColumn):
    def render(self, task):
        """渲染進度列顯示"""
        if task.total is None:
            return ""

        # 如果是主進度條
        if "Overall Progress" in task.description:
            return f"{task.completed} / {task.total} products"

        # 如果是檔案下載進度條
        completed = task.completed / (1024 * 1024)  # 轉換為 MB
        total = task.total / (1024 * 1024)
        return f"{completed:5.1f} / {total:5.1f} MB"


class SENTINEL5PHub(SatelliteHub):
    # API name
    name = "Sentinel-5P"

    def __init__(self, max_workers: int = 5):
        super().__init__()
        self._processor = None  # 初始化為 None，延遲創建

        self.auth = CopernicusAuth()
        self.downloader = Downloader()
        self.base_url = COPERNICUS_BASE_URL
        self.max_workers = max_workers
        self._token_lock = threading.Lock()
        self.download_stats = {
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'total_size': 0,
            'actual_download_size': 0,
        }

    def authentication(self):
        if not os.getenv('COPERNICUS_USERNAME') or not os.getenv('COPERNICUS_PASSWORD'):
            raise EnvironmentError(
                "Missing COPERNICUS credentials. Please set COPERNICUS_USERNAME and COPERNICUS_PASSWORD environment variables"
            )
        return SentinelAPI(os.getenv('COPERNICUS_USERNAME'), os.getenv('COPERNICUS_PASSWORD'))

    def fetch_data(self,
                   file_class: str | ClassInput,
                   file_type: str | TypeInput,
                   start_date: str | datetime,
                   end_date: str | datetime,
                   boundary: tuple[float, float, float, float] | None = None,
                   limit: int | None = None,
                   ) -> list[dict]:
        """
        擷取數據

        Args:
            file_class (ProductClassInput): 資料類型
            file_type (ProductTypeInput): 資料種類
            start_date: 開始日期 (YYYY-MM-DD)
            end_date: 結束日期 (YYYY-MM-DD)
            boundary: Geographic boundary (min_lon, max_lon, min_lat, max_lat)
            limit: 最大結果數量

        Returns:
            list[dict]: 產品資訊列表
        """
        self.file_class = file_class
        self.start_date, self.end_date = self._normalize_time_inputs(start_date, end_date, set_timezone=False)

        try:
            # 取得認證 token
            with self._token_lock:
                token = self.auth.ensure_valid_token()

            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }

            file_class = '' if file_class == '*' else file_class
            file_type = '' if file_type == '*' else file_type
            self.file_type = file_type

            # 構建基本篩選條件
            base_filter = (
                f"Collection/Name eq 'SENTINEL-5P' "
                f"and contains(Name,'{file_class}') "
                f"and contains(Name,'{file_type}') "
                f"and ContentDate/Start gt '{self.start_date.strftime('%Y-%m-%d')}T00:00:00.000Z' "
                f"and ContentDate/Start lt '{self.end_date.strftime('%Y-%m-%d')}T23:59:59.999Z' "
            )

            # 如果提供了邊界框，加入空間篩選
            if boundary:
                min_lon, max_lon, min_lat, max_lat = boundary
                spatial_filter = (
                    f" and OData.CSC.Intersects(area=geography'SRID=4326;POLYGON(("
                    f"{min_lon} {min_lat}, "
                    f"{max_lon} {min_lat}, "
                    f"{max_lon} {max_lat}, "
                    f"{min_lon} {max_lat}, "
                    f"{min_lon} {min_lat}))')"
                )
                base_filter += spatial_filter

            # 設置查詢參數
            query_params = {
                '$filter': base_filter,
                '$orderby': 'ContentDate/Start desc',
                '$top': limit if limit is not None else 200,
                '$skip': 0
            }

            all_products = []

            # 使用進度條顯示資料擷取進度
            with requests.Session() as session:  # 使用同步 HTTP 客戶端
                with Progress(
                        SpinnerColumn(),
                        TextColumn("[bold blue]{task.description}"),
                        BarColumn(bar_width=106),
                        FileProgressColumn(),
                        TimeRemainingColumn(),
                        console=console,
                        transient=True,
                        expand=True
                ) as progress:
                    fetch_task = progress.add_task(
                        "[cyan]Fetching products...",
                        total=None
                    )

                    while True:
                        try:
                            # 使用異步 HTTP 請求
                            with session.get(
                                    url=f"{self.base_url}/Products",
                                    headers=headers,
                                    params=query_params,
                                    timeout=DEFAULT_TIMEOUT
                            ) as response:
                                # 異步讀取響應
                                response_data = response.json()
                                products = response_data.get('value', [])

                                if not products:
                                    break

                                all_products.extend(products)

                            progress.update(
                                fetch_task,
                                description=f"[cyan]Found {len(all_products)} products..."
                            )

                            if limit and len(all_products) >= limit:
                                all_products = all_products[:limit]
                                break

                            query_params['$skip'] += len(products)

                        except Exception as e:
                            self.logger.error(f"Error fetching products: {str(e)}")
                            if len(all_products) > 0:
                                self.logger.info("Returning partially fetched products")
                                break
                            raise

                # 顯示產品詳細資訊
                if all_products:
                    DisplayManager().display_products(all_products)

                return all_products

        except Exception as e:
            self.logger.error(f"Error in fetch_no2_data: {str(e)}")
            raise

    def download_data(self, products: list, show_progress=False):
        """
        並行下載多個產品

        Parameters:
            products (list): 要下載的產品列表
            show_progress (bool): 是否顯示進度條，默認為True
        """
        if not products:
            self.logger.warning("No products to download")
            return

        # 使用 Queue 來管理下載任務
        import queue
        task_queue = queue.Queue()
        for product in products:
            task_queue.put(product)

        # 創建進度追蹤器
        completed_files = multiprocessing.Value('i', 0)
        active_threads = multiprocessing.Value('i', 0)

        # 初始化下載統計
        self.download_stats.update({
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'total_size': sum(p.get('ContentLength', 0) for p in products),
            'actual_download_size': 0,
            'start_time': time.time()
        })

        # 創建統計資料的鎖
        progress_lock = threading.Lock()
        stats_lock = threading.Lock()

        def download_files(progress, task_id, thread_index, completed_files, task_queue):
            try:
                with active_threads.get_lock():
                    active_threads.value += 1

                while True:
                    try:
                        # 非阻塞方式取得任務
                        product = task_queue.get_nowait()
                    except queue.Empty:
                        break

                    file_size = product.get('ContentLength', 0)
                    file_name = product.get('Name')

                    # 更新進度條顯示當前任務 (如果啟用進度條)
                    if show_progress and progress:
                        with progress_lock:
                            progress.update(
                                task_id,
                                description=f"[cyan]Thread {thread_index + 1}: {file_name[:28]}...{file_name[-9:]}",
                                total=file_size,
                                completed=0,
                                visible=True,
                                refresh=True
                            )
                    else:
                        # 不使用進度條時，使用日誌記錄進度
                        self.logger.info(f"Thread {thread_index + 1}: Downloading {file_name}")

                    success = False  # 用於追蹤是否需要呼叫 task_done()
                    try:
                        # 取得認證 token
                        with self._token_lock:
                            token = self.auth.ensure_valid_token()
                            headers = {'Authorization': f'Bearer {token}'}

                        product_type = self.file_type
                        start_time = product.get('ContentDate', {}).get('Start')
                        date_obj = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S.%fZ')
                        output_dir = self.raw_dir / product_type / date_obj.strftime('%Y') / date_obj.strftime('%m')
                        output_dir.mkdir(parents=True, exist_ok=True)

                        output_path = output_dir / file_name

                        # 檢查檔案是否已存在
                        if output_path.exists() and not zipfile.is_zipfile(output_path):
                            if show_progress and progress:
                                with progress_lock:
                                    progress.update(task_id, completed=file_size)
                            else:
                                self.logger.info(f"File already exists, skipping: {file_name}")

                            with stats_lock:
                                self.download_stats['skipped'] += 1
                            with completed_files.get_lock():
                                completed_files.value += 1
                            success = True
                            task_queue.task_done()
                            continue

                        product_id = product.get('Id')
                        download_url = f"{COPERNICUS_DOWNLOAD_URL}({product_id})/$value"

                        def update_progress(downloaded_bytes):
                            current_progress = min(downloaded_bytes, file_size)
                            if show_progress and progress:
                                with progress_lock:
                                    progress.update(task_id, completed=current_progress, refresh=True)
                            # 不使用進度條時，可以定期輸出日誌 (可選，這可能產生大量日誌)
                            # else:
                            #     if downloaded_bytes % (file_size // 10) < (file_size // 100):  # 每10%記錄一次
                            #         percent = int(downloaded_bytes / file_size * 100)
                            #         self.logger.info(f"Download progress for {file_name}: {percent}%")

                        # 執行下載
                        download_success = False
                        for attempt in range(3):
                            try:
                                if self.downloader.download_data(
                                        download_url,
                                        headers,
                                        output_path,
                                        progress_callback=update_progress if show_progress else None
                                ):
                                    download_success = True
                                    break

                                if not download_success and attempt < 2:
                                    time.sleep(5)
                                    with self._token_lock:
                                        token = self.auth.ensure_valid_token()
                                        headers = {'Authorization': f'Bearer {token}'}

                            except Exception as e:
                                self.logger.error(f"Download attempt {attempt + 1} failed for {file_name}: {str(e)}")
                                if attempt < 2:
                                    time.sleep(5)
                                continue

                        # 更新下載結果
                        with stats_lock:
                            if download_success:
                                self.download_stats['success'] += 1
                                self.logger.info(f"Successfully downloaded: {file_name}")
                            else:
                                self.download_stats['failed'] += 1
                                self.logger.error(f"Failed to download: {file_name}")
                                if output_path.exists():
                                    output_path.unlink()

                        success = True
                        with stats_lock:
                            self.download_stats['actual_download_size'] += file_size
                        with completed_files.get_lock():
                            completed_files.value += 1

                    except Exception as e:
                        self.logger.error(f"Error downloading {file_name}: {str(e)}")
                        with stats_lock:
                            self.download_stats['failed'] += 1
                        with completed_files.get_lock():
                            completed_files.value += 1

                        if 'output_path' in locals() and output_path.exists():
                            output_path.unlink()
                    finally:
                        if show_progress and progress:
                            with progress_lock:
                                progress.update(task_id, visible=False, refresh=True)
                        if not success:
                            task_queue.task_done()

            finally:
                with active_threads.get_lock():
                    active_threads.value -= 1
                if show_progress and progress:
                    with progress_lock:
                        progress.update(task_id, visible=False, refresh=True)

        # 根據是否顯示進度條執行不同的下載方式
        if show_progress:
            # 使用 rich 庫的進度條
            with Progress(
                    SpinnerColumn(),
                    TextColumn("[bold blue]{task.description}"),
                    BarColumn(complete_style="green"),
                    FileProgressColumn(),
                    TimeRemainingColumn(),
                    console=console,
                    expand=False,
                    transient=False
            ) as progress:
                # 創建主進度條
                main_task = progress.add_task("[green]Overall Progress", total=len(products))

                # 創建執行緒的進度條
                sub_tasks = []
                for i in range(self.max_workers):
                    task_id = progress.add_task(
                        f"[cyan]Thread {i + 1}: Waiting for download...",
                        total=100,
                        visible=True
                    )
                    sub_tasks.append(task_id)

                # 啟動下載執行緒
                threads = []
                for i, task_id in enumerate(sub_tasks):
                    thread = threading.Thread(
                        target=download_files,
                        args=(progress, task_id, i, completed_files, task_queue)
                    )
                    thread.daemon = True
                    threads.append(thread)
                    thread.start()
                    time.sleep(1)

                # 監控進度
                while True:
                    current_completed = completed_files.value
                    progress.update(main_task, completed=current_completed)

                    if (task_queue.empty() and
                            current_completed >= len(products) and
                            active_threads.value == 0):
                        break

                    time.sleep(0.1)

                # 確保所有進度條都被清理
                for task_id in sub_tasks:
                    progress.update(task_id, visible=False)

                # 顯示下載統計
                DisplayManager().display_download_summary(self.download_stats)
        else:
            # 不使用進度條，簡單的日誌輸出
            self.logger.info(f"Starting download of {len(products)} files...")

            # 啟動下載執行緒
            threads = []
            for i in range(self.max_workers):
                thread = threading.Thread(
                    target=download_files,
                    args=(None, None, i, completed_files, task_queue)
                )
                thread.daemon = True
                threads.append(thread)
                thread.start()

            # 等待下載完成
            total_files = len(products)
            while True:
                current_completed = completed_files.value
                if task_queue.empty() and current_completed >= total_files and active_threads.value == 0:
                    break

                # 定期輸出總體進度
                self.logger.info(f"Overall progress: {current_completed}/{total_files} files completed")
                time.sleep(5)  # 每5秒輸出一次總體進度

            # 顯示下載統計摘要
            self.logger.info(f"Download completed: {self.download_stats['success']} successful, "
                             f"{self.download_stats['failed']} failed, {self.download_stats['skipped']} skipped")

            # 如果有顯示管理器，也顯示完整統計
            if hasattr(self, 'display_manager'):
                self.display_manager.display_download_summary(self.download_stats)

    @property
    def processor(self):
        """延遲創建並返回SentinelProcessor實例"""
        if self._processor is None:
            # 確保file_type已被設置
            if not hasattr(self, 'file_type'):
                raise ValueError("未設置file_type，請先呼叫fetch_data方法")

            # 創建處理器實例
            self._processor = SentinelProcessor()

            # 設置路徑
            self._processor.raw_dir = self.raw_dir
            self._processor.processed_dir = self.processed_dir
            self._processor.figure_dir = self.figure_dir
            self._processor.geotiff_dir = self.geotiff_dir
            self._processor.logger = self.logger

        return self._processor

    def process_data(self, file_class=None, file_type=None, start_date=None, end_date=None):
        """處理下載的Sentinel-5P數據並生成可視化圖像"""
        if not hasattr(self, 'file_class'):
            raise ValueError("未設置file_class，請先呼叫fetch_data方法")

        # 確保file_type已被設置
        if not hasattr(self, 'file_type'):
            raise ValueError("未設置file_type，請先呼叫fetch_data方法")

        # 使用類屬性作為默認值
        if file_class is None:
            file_class = self.file_class
        if file_type is None:
            file_type = self.file_type
        if start_date is None:
            start_date = self.start_date
        if end_date is None:
            end_date = self.end_date

        # 準備日期範圍字符串用於日誌
        if start_date or end_date:
            date_range_str = f"日期範圍: {start_date if start_date else '最早'} 至 {end_date if end_date else '最新'}"
        else:
            date_range_str = "處理所有日期的數據"

        self.logger.info(f"開始處理Sentinel-5P數據，{date_range_str}")

        # 使用處理器處理所有文件
        return self.processor.process_each_data(file_class, file_type, start_date, end_date)


if __name__ == '__main__':
    sentinel_api = SentinelAPI(os.getenv('COPERNICUS_USERNAME'), os.getenv('COPERNICUS_PASSWORD'))