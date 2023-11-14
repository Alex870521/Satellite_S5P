"""Sentinel-5P API 操作"""
import logging
import time
import requests
import threading
import multiprocessing
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple

from rich.progress import (
    Progress,
    ProgressColumn,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TimeRemainingColumn,
)
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.align import Align

from src.api.auth import CopernicusAuth
from src.api.downloader import Downloader
from src.config.settings import (
    COPERNICUS_BASE_URL,
    COPERNICUS_DOWNLOAD_URL,
    RAW_DATA_DIR,
    DEFAULT_TIMEOUT
)

console = Console(force_terminal=True, color_system="auto", width=130)  # 使用您想要的寬度
logger = logging.getLogger(__name__)


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


class Sentinel5PDataFetcher:
    def __init__(self, max_workers: int = 5):
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
        }

    def fetch_no2_data(self, start_date: str, end_date: str,
                       bbox: Optional[Tuple[float, float, float, float]] = None,
                       limit: Optional[int] = None) -> List[Dict]:
        """
        擷取 NO2 數據

        Args:
            start_date: 開始日期 (YYYY-MM-DD)
            end_date: 結束日期 (YYYY-MM-DD)
            bbox: 邊界框座標 (min_lon, min_lat, max_lon, max_lat)
            limit: 最大結果數量

        Returns:
            List[Dict]: 產品資訊列表
        """
        # logger.info(f"Fetching NO2 data from {start_date} to {end_date}")

        try:
            # 取得認證 token
            with self._token_lock:
                token = self.auth.ensure_valid_token()

            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }

            # 構建基本篩選條件
            base_filter = (
                f"Collection/Name eq 'SENTINEL-5P' "
                f"and contains(Name,'NO2') "
                f"and ContentDate/Start gt {start_date}T00:00:00.000Z "
                f"and ContentDate/Start lt {end_date}T23:59:59.999Z"
            )

            # 如果提供了邊界框，加入空間篩選
            if bbox:
                min_lon, min_lat, max_lon, max_lat = bbox
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
                        response = requests.get(
                            f"{self.base_url}/Products",
                            headers=headers,
                            params=query_params,
                            timeout=DEFAULT_TIMEOUT
                        )
                        response.raise_for_status()

                        products = response.json().get('value', [])
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

                    except requests.exceptions.RequestException as e:
                        logger.error(f"Error fetching products: {str(e)}")
                        if len(all_products) > 0:
                            logger.info("Returning partially fetched products")
                            break
                        raise

            # 顯示產品詳細資訊
            if all_products:
                table = Table(title=f"Found {len(all_products)} Products")
                table.add_column("No.", justify="right", style="cyan")
                table.add_column("Time", style="magenta")
                table.add_column("Name", style="blue")
                table.add_column("Size", justify="right", style="green")

                for i, product in enumerate(all_products, 1):
                    time_str = product.get('ContentDate', {}).get('Start', 'N/A')[:19]
                    name = product.get('Name', 'N/A')
                    size = product.get('ContentLength', 0)
                    size_str = f"{size / 1024 / 1024:.2f} MB"
                    table.add_row(str(i), time_str, name, size_str)

                console.print(table)

            return all_products

        except Exception as e:
            logger.error(f"Error in fetch_no2_data: {str(e)}")
            raise

    # TODO: main_task count wrong
    def parallel_download(self, products: list):
        """並行下載多個產品"""
        if not products:
            logger.warning("No products to download")
            return

        # 使用 Queue 來管理下載任務
        import queue
        task_queue = queue.Queue()
        for product in products:
            task_queue.put(product)

        # 創建進度追蹤器
        import multiprocessing
        completed_files = multiprocessing.Value('i', 0)
        active_threads = multiprocessing.Value('i', 0)

        # 初始化下載統計
        self.download_stats.update({
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'total_size': sum(p.get('ContentLength', 0) for p in products),
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

                    # 更新進度條顯示當前任務
                    with progress_lock:
                        progress.update(
                            task_id,
                            description=f"[cyan]Thread {thread_index + 1}: {file_name}",
                            total=file_size,
                            completed=0,
                            visible=True,
                            refresh=True
                        )

                    success = False  # 用於追蹤是否需要呼叫 task_done()
                    try:
                        # 取得認證 token
                        with self._token_lock:
                            token = self.auth.ensure_valid_token()
                            headers = {'Authorization': f'Bearer {token}'}

                        start_time = product.get('ContentDate', {}).get('Start')
                        date_obj = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S.%fZ')
                        output_dir = Path(RAW_DATA_DIR) / date_obj.strftime('%Y') / date_obj.strftime('%m')
                        output_path = output_dir / file_name

                        # 檢查檔案是否已存在
                        if output_path.exists() and output_path.stat().st_size == file_size:
                            with progress_lock:
                                progress.update(task_id, completed=file_size)
                            with stats_lock:
                                self.download_stats['skipped'] += 1
                            with completed_files.get_lock():
                                completed_files.value += 1
                            success = True
                            task_queue.task_done()
                            continue

                        output_dir.mkdir(parents=True, exist_ok=True)
                        product_id = product.get('Id')
                        download_url = f"{COPERNICUS_DOWNLOAD_URL}({product_id})/$value"

                        def update_progress(downloaded_bytes):
                            current_progress = min(downloaded_bytes, file_size)
                            with progress_lock:
                                progress.update(task_id, completed=current_progress, refresh=True)

                        # 執行下載
                        download_success = False
                        for attempt in range(3):
                            try:
                                if self.downloader.download_file(
                                        download_url,
                                        headers,
                                        output_path,
                                        progress_callback=update_progress
                                ):
                                    download_success = True
                                    break

                                if not download_success and attempt < 2:
                                    time.sleep(5)
                                    with self._token_lock:
                                        token = self.auth.ensure_valid_token()
                                        headers = {'Authorization': f'Bearer {token}'}

                            except Exception as e:
                                logger.error(f"Download attempt {attempt + 1} failed for {file_name}: {str(e)}")
                                if attempt < 2:
                                    time.sleep(5)
                                continue

                        # 更新下載結果
                        with stats_lock:
                            if download_success:
                                self.download_stats['success'] += 1
                            else:
                                self.download_stats['failed'] += 1
                                if output_path.exists():
                                    output_path.unlink()

                        success = True
                        with completed_files.get_lock():
                            completed_files.value += 1

                    except Exception as e:
                        logger.error(f"Error downloading {file_name}: {str(e)}")
                        with stats_lock:
                            self.download_stats['failed'] += 1
                        with completed_files.get_lock():
                            completed_files.value += 1

                        if 'output_path' in locals() and output_path.exists():
                            output_path.unlink()
                    finally:
                        with progress_lock:
                            progress.update(task_id, visible=False, refresh=True)
                        if not success:
                            task_queue.task_done()

            finally:
                with active_threads.get_lock():
                    active_threads.value -= 1
                with progress_lock:
                    progress.update(task_id, visible=False, refresh=True)

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

            # 確保主進度條顯示完成
            # progress.update(main_task, completed=len(products), refresh=True)

            # 顯示下載統計
            self._display_download_summary()

    def _display_download_summary(self):
        """顯示下載統計摘要"""
        elapsed_time = time.time() - self.download_stats['start_time']
        total_files = (
            self.download_stats['success'] +
            self.download_stats['failed'] +
            self.download_stats['skipped']
        )

        table = Table(title="Download Summary", width=60, padding=(0, 2), expand=False)
        table.add_column("Metric", style="cyan")
        table.add_column("Value", justify="right", style="green")

        table.add_row("Total Files", str(total_files))
        table.add_row("Successfully Downloaded", str(self.download_stats['success']))
        table.add_row("Failed Downloads", str(self.download_stats['failed']))
        table.add_row("Skipped Files", str(self.download_stats['skipped']))

        total_size = self.download_stats['total_size']
        size_str = f"{total_size / 1024 / 1024:.2f} MB"
        table.add_row("Total Size", size_str)
        table.add_row("Total Time", f"{elapsed_time:.2f}s")

        if elapsed_time > 0:
            avg_speed = total_size / elapsed_time
            speed_str = f"{avg_speed / 1024 / 1024:.2f} MB/s"
            table.add_row("Average Speed", speed_str)

        # 使用 Align 將 table 置中
        centered_table = Align.center(table)

        console.print("\n", Panel(
            centered_table,
            title="Download Results",
            width=130,
            expand=True,
            border_style="bright_blue",
            padding=(1, 0)
        ))
