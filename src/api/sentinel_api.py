import os
import time
import aiohttp  # 異步操作
import requests
import zipfile
import queue
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
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
from src.config.richer import progress_console, DisplayManager
from src.config.catalog import ClassInput, TypeInput, PRODUCT_CONFIGS
from src.api.core import SatelliteHub
from src.processing import SentinelProcessor
from sentinelsat import SentinelAPI


class FileProgressColumn(ProgressColumn):
    def render(self, task):
        """渲染進度列：主進度條顯示已完成產品數，檔案進度條顯示 MB。

        以 task.fields['kind'] 區分主/子進度條，不再用描述字串嗅探。
        """
        if task.fields.get("kind") == "overall":
            return f"{int(task.completed)} / {int(task.total)} products"

        if task.total is None:
            return ""

        # 檔案下載進度條：bytes → MB
        completed = task.completed / (1024 * 1024)
        total = task.total / (1024 * 1024)
        return f"{completed:5.1f} / {total:5.1f} MB"


class SentinelHubBase(SatelliteHub):
    """Sentinel 衛星數據的基礎類別"""

    # 子類別需要覆寫這些屬性
    name = None  # 例如: "Sentinel-5P", "Sentinel-3"
    collection_name = None  # 例如: "SENTINEL-5P", "SENTINEL-3"

    def __init__(self, max_workers: int = 5, region: str = 'taiwan'):
        super().__init__(region=region)
        self._processor = None  # 初始化為 None，延遲創建

        self.auth = CopernicusAuth()
        self.downloader = Downloader(manifest_dir=self.raw_dir if hasattr(self, 'raw_dir') and self.raw_dir else None)
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
                f"Collection/Name eq '{self.collection_name}' "
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
                        BarColumn(),  # 自適應寬度，避免寫死寬度在窄終端機折行
                        FileProgressColumn(),
                        TimeRemainingColumn(),
                        console=progress_console,
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

                if not all_products:
                    print("There is no products to fetch. Please check the File class options.")

                return all_products

        except Exception as e:
            self.logger.error(f"Error in fetch_no2_data: {str(e)}")
            raise

    def download_data(self, products: list, show_progress=False):
        """並行下載多個產品（ThreadPoolExecutor）。

        Parameters:
            products (list): 要下載的產品列表
            show_progress (bool): 是否顯示 rich 進度條。預設 False（headless/
                automation 走日誌輸出，避免把進度條灌進 pipeline log）。

        進度顯示維持固定 max_workers 條 worker 列、原地刷新：每個正在執行的
        download_one 從 slot_pool 借一條列，完成後歸還，不新增/移除任何列
        （避免 live 區域高度變動導致畫面往下捲）。
        """
        if not products:
            self.logger.warning("No products to download")
            return

        # 初始化下載統計
        self.download_stats.update({
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'total_size': sum(p.get('ContentLength', 0) for p in products),
            'actual_download_size': 0,
            'start_time': time.time()
        })

        stats_lock = threading.Lock()

        def download_one(product, progress, slot_pool, sub_tasks):
            """下載單一產品。stats 更新自帶鎖；progress=None 時走日誌。"""
            file_size = product.get('ContentLength', 0)
            file_name = product.get('Name')

            # 借一條固定的 worker 進度列（原地刷新，跑完歸還）
            slot = task_id = None
            if progress is not None:
                slot = slot_pool.get()
                task_id = sub_tasks[slot]
                progress.update(
                    task_id,
                    description=f"[cyan]Thread {slot + 1}: {file_name[:28]}...{file_name[-9:]}",
                    total=file_size or None,
                    completed=0,
                )
            else:
                self.logger.info(f"Downloading {file_name}")

            output_path = None
            try:
                # 取得認證 token
                with self._token_lock:
                    token = self.auth.ensure_valid_token()
                    headers = {'Authorization': f'Bearer {token}'}

                product_type = self.file_type
                start_time = product.get('ContentDate', {}).get('Start')
                date_obj = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S.%fZ')
                output_dir = self.raw_dir / 'L2' / product_type / date_obj.strftime('%Y') / date_obj.strftime('%m')
                output_dir.mkdir(parents=True, exist_ok=True)
                output_path = output_dir / file_name

                # 檔案已存在（已解壓的 .nc，非 zip）→ 跳過
                if output_path.exists() and not zipfile.is_zipfile(output_path):
                    if progress is not None:
                        progress.update(task_id, total=file_size or 1, completed=file_size or 1)
                    else:
                        self.logger.info(f"File already exists, skipping: {file_name}")
                    with stats_lock:
                        self.download_stats['skipped'] += 1
                    return

                product_id = product.get('Id')
                download_url = f"{COPERNICUS_DOWNLOAD_URL}({product_id})/$value"

                def update_progress(downloaded_bytes):
                    if progress is not None:
                        progress.update(task_id, completed=min(downloaded_bytes, file_size))

                # 執行下載（最多重試 3 次，失敗間隔 5s 並刷新 token）
                download_success = False
                for attempt in range(3):
                    try:
                        if self.downloader.download_data(
                                download_url,
                                headers,
                                output_path,
                                progress_callback=update_progress if progress is not None else None
                        ):
                            download_success = True
                            break

                        if attempt < 2:
                            time.sleep(5)
                            with self._token_lock:
                                token = self.auth.ensure_valid_token()
                                headers = {'Authorization': f'Bearer {token}'}

                    except Exception as e:
                        self.logger.error(f"Download attempt {attempt + 1} failed for {file_name}: {str(e)}")
                        if attempt < 2:
                            time.sleep(5)
                        continue

                with stats_lock:
                    if download_success:
                        self.download_stats['success'] += 1
                        self.download_stats['actual_download_size'] += file_size
                        self.logger.info(f"Successfully downloaded: {file_name}")
                    else:
                        self.download_stats['failed'] += 1
                        self.logger.error(f"Failed to download: {file_name}")
                        if output_path.exists():
                            output_path.unlink()

            except Exception as e:
                self.logger.error(f"Error downloading {file_name}: {str(e)}")
                with stats_lock:
                    self.download_stats['failed'] += 1
                if output_path is not None and output_path.exists():
                    output_path.unlink()
            finally:
                # 歸還 worker 列：重設為等待狀態，留在原位（不移除）
                if progress is not None:
                    progress.update(
                        task_id,
                        description=f"[cyan]Thread {slot + 1}: Waiting for download...",
                        total=1,
                        completed=0,
                    )
                    slot_pool.put(slot)

        # 只有「要求進度條」且「真的是互動終端機」時才畫 thread 進度條；
        # 非互動（被 `!`/管線/重導向捕捉、cron）時自動退回逐檔文字狀態（走 logger，
        # 看得到下載狀況），不顯示 thread bars——避免在非 TTY 折行洗版。
        use_bars = show_progress and progress_console.is_terminal

        if use_bars:
            with Progress(
                    SpinnerColumn(),
                    TextColumn("[bold blue]{task.description}"),
                    BarColumn(complete_style="green"),
                    FileProgressColumn(),
                    TimeRemainingColumn(),
                    console=progress_console,
                    expand=False,
                    transient=False
            ) as progress:
                overall = progress.add_task("[green]Overall Progress", total=len(products), kind="overall")

                # 固定 max_workers 條 worker 列 + 對應的 slot 池
                sub_tasks = [
                    progress.add_task(f"[cyan]Thread {i + 1}: Waiting for download...", total=1, completed=0)
                    for i in range(self.max_workers)
                ]
                slot_pool = queue.Queue()
                for i in range(self.max_workers):
                    slot_pool.put(i)

                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [executor.submit(download_one, p, progress, slot_pool, sub_tasks) for p in products]
                    for _ in as_completed(futures):
                        progress.advance(overall)

                for task_id in sub_tasks:
                    progress.update(task_id, visible=False)

                DisplayManager().display_download_summary(self.download_stats)
        else:
            self.logger.info(f"Starting download of {len(products)} files...")
            completed = 0
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [executor.submit(download_one, p, None, None, None) for p in products]
                for _ in as_completed(futures):
                    completed += 1
                    self.logger.info(f"Overall progress: {completed}/{len(products)} files completed")

            self.logger.info(f"Download completed: {self.download_stats['success']} successful, "
                             f"{self.download_stats['failed']} failed, {self.download_stats['skipped']} skipped")

            # 非互動也顯示統計面板（已自適應寬度，會以純文字輸出）
            DisplayManager().display_download_summary(self.download_stats)


    @property
    def processor(self):
        """延遲創建並返回SentinelProcessor實例"""
        if self._processor is None:
            # 確保file_type已被設置
            if not hasattr(self, 'file_type'):
                raise ValueError("未設置file_type，請先呼叫fetch_data方法")

            # 創建處理器實例（用區域的格網 bounds，解析度沿用各產品預設）
            self._processor = SentinelProcessor(bounds=self.region_bounds)

            # 設置路徑
            self._processor.raw_dir = self.raw_dir
            self._processor.processed_dir = self.processed_dir
            self._processor.figure_dir = self.figure_dir
            self._processor.geotiff_dir = self.geotiff_dir
            self._processor.logger = self.logger
            self._processor.file_type = self.file_type
            self._processor.file_class = self.file_class

        return self._processor

    def process_data(self, pattern=None, start_date=None, end_date=None):
        """處理下載的Sentinel數據並生成可視化圖像"""
        if not hasattr(self, 'file_class'):
            raise ValueError("未設置file_class，請先呼叫fetch_data方法")

        # 確保file_type已被設置
        if not hasattr(self, 'file_type'):
            raise ValueError("未設置file_type，請先呼叫fetch_data方法")

        # 如果未指定模式，使用基於file_type的默認模式
        if pattern is None:
            pattern = f"**/{self.file_type}/**/*{self.file_class}*.nc"

        # 使用類屬性作為默認值
        if start_date is None:
            start_date = self.start_date
        if end_date is None:
            end_date = self.end_date

        # 準備日期範圍字符串用於日誌
        if start_date or end_date:
            date_range_str = f"日期範圍: {start_date if start_date else '最早'} 至 {end_date if end_date else '最新'}"
        else:
            date_range_str = "處理所有日期的數據"

        self.logger.info(f"開始處理{self.name}數據，{date_range_str}")

        # 使用處理器處理所有文件
        return self.processor.process_all_files(pattern, start_date, end_date)

    # ------------------------------------------------------------------ #
    # Level-3 (S5P-PAL gridded) — 官方做好的全球 L3，免金鑰、走 STAC。
    # 與 L2 路徑(Copernicus OData)並存:L2 自己內插成台灣網格;L3 直接下載官方
    # 全球網格再裁區域。詳見 [[s5p-pal-l3]] / src/api/s5p_pal.py。
    # ------------------------------------------------------------------ #
    # 友善/L2 代碼 -> S5P-PAL L3 product id（NO2 對齊對流層柱)
    L3_PRODUCT_MAP = {
        'NO2___': 'no2-tropospheric', 'NO2': 'no2-tropospheric',
        'O3____': 'o3', 'O3': 'o3',
        'HCHO__': 'hcho', 'HCHO': 'hcho',
        'SO2___': 'so2-7km', 'SO2': 'so2-7km',
        'CO____': 'co', 'CO': 'co',
        'CH4___': 'ch4', 'CH4': 'ch4',
        'AER_AI': 'aai', 'AER_LH': 'alh',
    }

    def _l3_product_id(self, product: str) -> str:
        """L2/友善代碼 → L3 product id；已是 L3 id 則原樣回傳。"""
        return self.L3_PRODUCT_MAP.get(product, product)

    def fetch_l3(self, product: str, aggregation: str,
                 start_date, end_date) -> list[dict]:
        """查 S5P-PAL L3 清單(不下載)。aggregation: day/fortnight/month/season/year。"""
        from src.api.s5p_pal import S5PPALClient
        pid = self._l3_product_id(product)
        items = S5PPALClient(logger=self.logger).find_items(pid, aggregation, start_date, end_date)
        self.logger.info(f"S5P-PAL L3 {pid}/{aggregation}: 找到 {len(items)} 筆({start_date}~{end_date})")
        return items

    def run_l3_pipeline(self, product: str, aggregation: str, start_date, end_date,
                        keep_global: bool = False, crop: bool = True,
                        skip_existing: bool = True, limit: int | None = None,
                        max_workers: int = 1) -> list:
        """L3 串流式:逐檔「下載全球 → 裁區域(self.region) → 可刪全球」。

        全球 L3 每檔大(日~480MB、月~1GB),裁台灣後極小;keep_global=False(預設)
        裁完即刪全球原始,磁碟用量持平。輸出:
            裁切檔 -> processed_dir/L3/{product}/{agg}/{id}.nc
            全球檔 -> raw_dir/L3/{product}/{agg}/{id}.nc(keep_global=True 才保留)

        max_workers>1:**並發下載**(瓶頸是 S5P-PAL 每連線限速 ~60-80Mbps,實測並行
            3 條 ≈ 2.1×)。下載並發、但**裁切/寫檔在主執行緒序列**處理(netcdf 寫入
            非執行緒安全)。keep_global=False 時磁碟峰值 ≈ max_workers 個全球檔。建議 3~4。
        單檔失敗只記錄、不中斷整批;skip_existing 讓重跑便宜(冪等)。
        """
        from src.api.s5p_pal import S5PPALClient
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import xarray as xr

        pid = self._l3_product_id(product)
        client = S5PPALClient(logger=self.logger)
        items = client.find_items(pid, aggregation, start_date, end_date)
        if limit is not None:
            items = items[:limit]
        if not items:
            self.logger.warning("S5P-PAL L3: 無符合資料(檢查 product/aggregation/日期)")
            return []

        out_proc = self.processed_dir / 'L3' / pid / aggregation
        out_glob = self.raw_dir / 'L3' / pid / aggregation
        lon_min, lon_max, lat_min, lat_max = self.region_bounds

        # 1) 先濾掉已完成/無連結,留下要下載的 todo
        results = []
        todo = []
        for it in items:
            cropped = out_proc / f"{it['id']}.nc"
            if skip_existing and cropped.exists() and cropped.stat().st_size > 0:
                results.append(cropped)
                continue
            if not it.get('href'):
                self.logger.warning(f"無下載連結,跳過: {it['id']}")
                continue
            todo.append(it)
        n = len(todo)

        def _download(it):
            try:
                return it, client.download(it['href'], out_glob / f"{it['id']}.nc"), None
            except Exception as ex:
                return it, None, ex

        # 主執行緒序列:裁切 + 寫檔 + 視情況刪全球(netcdf 寫入非執行緒安全)
        def _finalize(it, gpath, err, idx):
            if err or gpath is None:
                self.logger.error(f"[{idx}/{n}] 下載失敗(略過續跑): {it['id']} — {type(err).__name__ if err else 'no path'}: {err}")
                if not keep_global:
                    (out_glob / f"{it['id']}.nc").unlink(missing_ok=True)
                    (out_glob / f"{it['id']}.nc.part").unlink(missing_ok=True)
                return
            try:
                self.logger.info(f"[{idx}/{n}] 完成下載: {it['id']} ({gpath.stat().st_size/1e6:.0f} MB)")
                if not crop:
                    results.append(gpath)
                    return
                cropped = out_proc / f"{it['id']}.nc"
                ds = xr.open_dataset(gpath)
                asc = bool(ds.latitude.values[1] > ds.latitude.values[0])
                sub = ds.sel(
                    longitude=slice(lon_min, lon_max),
                    latitude=slice(lat_min, lat_max) if asc else slice(lat_max, lat_min),
                )
                out_proc.mkdir(parents=True, exist_ok=True)
                sub.to_netcdf(cropped)
                ds.close()
                self.logger.info(f"   裁切 {self.region} 存檔: {cropped.name} {dict(sub.sizes)}")
                results.append(cropped)
                if not keep_global:
                    gpath.unlink(missing_ok=True)
            except Exception as ex:
                self.logger.error(f"[{idx}/{n}] 裁切失敗(略過續跑): {it['id']} — {type(ex).__name__}: {ex}")
                if not keep_global:
                    gpath.unlink(missing_ok=True)

        # 2) 下載(並發或序列),裁切一律在主執行緒序列
        if max_workers and max_workers > 1:
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futs = [ex.submit(_download, it) for it in todo]
                for idx, fut in enumerate(as_completed(futs), 1):
                    it, gpath, err = fut.result()
                    _finalize(it, gpath, err, idx)
        else:
            for idx, it in enumerate(todo, 1):
                self.logger.info(f"[{idx}/{n}] 下載全球 L3: {it['id']}")
                it, gpath, err = _download(it)
                _finalize(it, gpath, err, idx)
        return results


class SENTINEL5PHub(SentinelHubBase):
    """Sentinel-5P 衛星數據 API"""
    name = "Sentinel-5P"
    collection_name = "SENTINEL-5P"


class SENTINEL3Hub(SentinelHubBase):
    """Sentinel-3 衛星數據 API"""
    name = "Sentinel-3"
    collection_name = "SENTINEL-3"


if __name__ == '__main__':
    sentinel_api = SentinelAPI(os.getenv('COPERNICUS_USERNAME'), os.getenv('COPERNICUS_PASSWORD'))