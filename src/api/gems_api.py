import os
import time
from datetime import datetime
from pathlib import Path
from typing import Literal, Optional, List, Dict, Any

import requests

from src.api.core import SatelliteHub
from src.config.richer import DisplayManager, make_download_progress


class GEMSHub(SatelliteHub):
    """
    GEMS (Geostationary Environment Monitoring Spectrometer) API Hub.

    GEMS 是韓國 GK-2B 衛星上的地球同步大氣環境監測光譜儀，監測東亞地區的
    大氣污染物（NO2、O3、HCHO、SO2、氣溶膠等），白天每小時觀測一次。

    資料來源：NIER 環境衛星中心 (NESC) Open-API。
    需在 https://nesc.nier.go.kr 申請 API key，並設定環境變數 GEMS_API_KEY。

    真實 API 規格（2026-06 實測確認）：
        base   = https://nesc.nier.go.kr:38032
        list   = {base}/api/GK2/{level}/{type}/data/getFileList.do
        dates  = {base}/api/GK2/{level}/{type}/data/getFileDateList.do
        item   = {base}/api/GK2/{level}/{type}/data/getFileItem.do?date=yyyyMMddHHmmss
        params = sDate / eDate / date (yyyyMMddHHmm[ss]), ver, format=json, key
        注意：路徑上的衛星段是 "GK2"，"GEMS" 是 selectVersion 查詢時用的 instrument 代碼。
    """
    name = "GEMS"

    # API endpoint
    BASE_URL = "https://nesc.nier.go.kr:38032"
    SATELLITE = "GK2"  # 路徑段（GK-2B）。版本查詢用的 instrument 代碼則是 "GEMS"
    # 版本查詢走主站 AJAX（與資料下載不同 host）
    VERSION_URL = "https://nesc.nier.go.kr/en/data/openapi/openApi/selectVersion.do"

    # 對外友善名稱 -> NESC 產品代碼。GEMS 為 UV-Vis 光譜儀，不量測 CO/CH4。
    PRODUCT_TYPES = {
        'NO2': 'NO2',          # 二氧化氮
        'O3': 'O3T',           # 臭氧總量（O3T）
        'O3T': 'O3T',          # 臭氧總量
        'O3P': 'O3P',          # 臭氧垂直分布
        'HCHO': 'HCHO',        # 甲醛
        'CHOCHO': 'CHOCHO',    # 乙二醛
        'SO2': 'SO2',          # 二氧化硫
        'AOD': 'AERAOD',       # 氣溶膠光學厚度
        'AERAOD': 'AERAOD',
        'UVI': 'UVI',          # 紫外線指數
        'AEH': 'AEH',          # 氣溶膠有效高度
        'CLOUD': 'CLOUD',      # 雲資訊
    }

    # 已知預設版本（fetch_data 未指定 ver 時，會先嘗試線上查最新版，失敗才用這裡）
    DEFAULT_VERSIONS = {
        'NO2': 'v4.0.1',
    }

    # 預設地理邊界 (東亞地區)。注意：getFileItem 回傳的是全盤 (full-disc) granule，
    # 此邊界僅供後續處理/裁切參考，API 查詢/下載階段不使用。
    DEFAULT_BOUNDARY = (100.0, 0.0, 150.0, 50.0)

    def __init__(self, max_workers: int = 3):
        super().__init__()
        self.max_workers = max_workers
        self._processor = None
        self._reset_stats()

    def _reset_stats(self):
        self.download_stats = {
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'total_size': 0,
            'actual_download_size': 0,
        }

    # ------------------------------------------------------------------ #
    # Authentication
    # ------------------------------------------------------------------ #
    def authentication(self):
        """
        讀取 GEMS_API_KEY 並向 NESC 驗證，回傳已帶 key 的 requests.Session。

        無 key 時回傳 None（fetch/download 會在使用時報錯），與其他 Hub 行為一致。
        """
        api_key = os.getenv('GEMS_API_KEY')
        if not api_key:
            self.logger.warning(
                "GEMS_API_KEY not set — 請至 https://nesc.nier.go.kr 申請並設定環境變數 GEMS_API_KEY"
            )
            self.api_key = None
            return None

        self.api_key = api_key.strip()
        session = requests.Session()

        # 用 getKeyInfo.do 驗證 key 是否有效（回傳 XML）
        try:
            resp = session.get(
                f"{self.BASE_URL}/api/getKeyInfo.do",
                params={'key': self.api_key},
                timeout=30,
            )
            if 'SuccessYN>Y' in resp.text or '<useYn>Y</useYn>' in resp.text:
                self.logger.info("GEMS API key 驗證成功")
            else:
                self.logger.warning(f"GEMS API key 驗證未通過：{resp.text[:200]}")
        except requests.RequestException as e:
            self.logger.warning(f"無法驗證 GEMS API key（將繼續嘗試）：{e}")

        return session

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _require_client(self):
        if self.client is None or not getattr(self, 'api_key', None):
            raise RuntimeError("缺少 GEMS_API_KEY，無法存取 GEMS Open-API")

    def _resolve_product_code(self, product_type: str) -> str:
        if product_type in self.PRODUCT_TYPES:
            return self.PRODUCT_TYPES[product_type]
        # 也允許直接傳入原生 NESC 代碼
        if product_type in self.PRODUCT_TYPES.values():
            return product_type
        raise ValueError(
            f"不支援的產品類型: {product_type}. 支援: {sorted(self.PRODUCT_TYPES)}"
        )

    def _latest_version(self, product_code: str, level: str) -> Optional[str]:
        """線上查詢某產品的可用版本，回傳最新（字串排序最大）版本；失敗回傳 None。"""
        try:
            resp = requests.get(
                self.VERSION_URL,
                params={
                    'svcSe': '04',
                    'sat': 'GEMS',      # 版本查詢用 instrument 代碼
                    'lvl': level,
                    'type': product_code,
                    'frmatSe': '02',    # 02 = data (NetCDF)
                },
                headers={'X-Requested-With': 'XMLHttpRequest'},
                timeout=30,
            )
            versions = [d.get('VER_INFO') for d in resp.json().get('data', []) if d.get('VER_INFO')]
            if versions:
                return sorted(versions)[-1]
        except (requests.RequestException, ValueError) as e:
            self.logger.debug(f"線上版本查詢失敗：{e}")
        return None

    def _resolve_version(self, product_type: str, product_code: str, level: str,
                         ver: Optional[str]) -> str:
        if ver:
            return ver
        ver = self._latest_version(product_code, level)
        if ver:
            self.logger.info(f"使用線上查得的最新版本 ver={ver}")
            return ver
        ver = self.DEFAULT_VERSIONS.get(product_type) or self.DEFAULT_VERSIONS.get(product_code)
        if ver:
            self.logger.info(f"使用預設版本 ver={ver}")
            return ver
        raise ValueError(
            f"無法決定 {product_code} 的版本，請以 ver= 明確指定（如 'v4.0.1'）"
        )

    def _api_url(self, product_code: str, level: str, method: str) -> str:
        return f"{self.BASE_URL}/api/{self.SATELLITE}/{level}/{product_code}/data/{method}"

    def _get_json(self, url: str, params: dict) -> dict:
        """送出查詢並處理 NESC 的錯誤包封 (errorCode/errorMessage)。"""
        resp = self.client.get(url, params={**params, 'key': self.api_key}, timeout=60)
        try:
            data = resp.json()
        except ValueError:
            raise RuntimeError(f"GEMS API 非預期回應 (HTTP {resp.status_code}): {resp.text[:200]}")
        if isinstance(data, dict) and 'errorCode' in data:
            code = data.get('errorCode')
            msg = data.get('errorMessage', '')
            # errorCode 3 = 查無資料，視為空結果而非錯誤
            if str(code) == '3':
                return {'list': []}
            raise RuntimeError(f"GEMS API 錯誤 [{code}]: {msg}")
        return data

    @staticmethod
    def _filename_to_datetime(filename: str) -> Optional[datetime]:
        """GK2_GEMS_L2_20230515_0345_NO2_FW_DPRO_ORI.nc -> datetime(2023,5,15,3,45)"""
        parts = filename.split('_')
        for i, p in enumerate(parts):
            if len(p) == 8 and p.isdigit() and i + 1 < len(parts):
                hhmm = parts[i + 1]
                if len(hhmm) == 4 and hhmm.isdigit():
                    try:
                        return datetime.strptime(p + hhmm, '%Y%m%d%H%M')
                    except ValueError:
                        return None
        return None

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #
    def fetch_data(self,
                   product_type: str | Literal['NO2', 'O3', 'O3T', 'O3P', 'HCHO', 'CHOCHO',
                                               'SO2', 'AOD', 'AERAOD', 'UVI', 'AEH', 'CLOUD'],
                   start_date: str | datetime,
                   end_date: str | datetime,
                   ver: Optional[str] = None,
                   level: str = 'L2',
                   boundary: tuple = DEFAULT_BOUNDARY,
                   limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        查詢 GEMS 產品檔案清單。

        Parameters:
            product_type: 產品類型（友善名或原生代碼，見 PRODUCT_TYPES）
            start_date, end_date: 觀測時間範圍（str 或 datetime）
            ver: 產品版本（如 'v4.0.1'）。None 時自動查最新版
            level: 產品階層，預設 'L2'
            boundary: 保留參數（GEMS granule 為全盤，查詢階段不使用）
            limit: 最多回傳幾筆

        Returns:
            List[Dict]: 每筆含 Name / ContentDate / date / ver / ProductType 等欄位
        """
        self._require_client()
        self.product_type = product_type
        self.level = level
        self.start_date, self.end_date = self._normalize_time_inputs(start_date, end_date, set_timezone=False)

        product_code = self._resolve_product_code(product_type)
        self.version = self._resolve_version(product_type, product_code, level, ver)

        s = self.start_date.strftime('%Y%m%d%H%M')
        e = self.end_date.strftime('%Y%m%d%H%M')

        self.logger.info(f"正在查詢 GEMS {product_code} ({level}, {self.version}) 數據...")
        self.logger.info(f"時間範圍: {self.start_date} 至 {self.end_date}")

        url = self._api_url(product_code, level, 'getFileList.do')
        data = self._get_json(url, {'sDate': s, 'eDate': e, 'ver': self.version, 'format': 'json'})

        items = data.get('list', []) if isinstance(data, dict) else []
        products: List[Dict[str, Any]] = []
        for entry in items:
            filename = entry.get('item') if isinstance(entry, dict) else entry
            if not filename:
                continue
            dt = self._filename_to_datetime(filename)
            date_param = dt.strftime('%Y%m%d%H%M%S') if dt else None
            start_iso = dt.strftime('%Y-%m-%dT%H:%M:%S.000Z') if dt else ''
            products.append({
                'Id': filename,
                'Name': filename,
                'ContentLength': 0,  # 大小在下載時由 HTTP header 取得
                'ContentDate': {'Start': start_iso, 'End': start_iso},
                'ProductType': product_code,
                'Satellite': 'GEMS',
                'ver': self.version,
                'level': level,
                'date': date_param,
            })

        products.sort(key=lambda p: p['date'] or '')
        if limit is not None:
            products = products[:limit]

        if products:
            DisplayManager().display_products(products)
            self.logger.info(f"找到 {len(products)} 個 GEMS 產品")
        else:
            self.logger.warning("未找到符合條件的 GEMS 產品（檢查日期/版本/產品是否有覆蓋）")

        return products

    def _stream_response_to_file(self, response, out_path: Path, progress=None, task=None) -> int:
        """把已開啟的串流回應寫到 out_path（先寫 .part 再 rename），回傳寫入位元組數。

        download_data 與 _download_granule 共用的串流核心；Content-Type 驗證、
        錯誤/空資料判斷仍由各呼叫端處理（屬 GEMS 專屬邏輯）。
        """
        tmp_path = out_path.with_suffix(out_path.suffix + '.part')
        written = 0
        with open(tmp_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1 << 20):
                if chunk:
                    f.write(chunk)
                    written += len(chunk)
                    if progress is not None and task is not None:
                        progress.update(task, advance=len(chunk))
        tmp_path.rename(out_path)
        return written

    def download_data(self, products: List[Dict], show_progress: bool = True) -> List[str]:
        """下載 GEMS 產品（逐檔串流寫入 raw_dir）。"""
        self._require_client()
        if not products:
            self.logger.warning("沒有產品需要下載")
            return []

        self.logger.info(f"開始下載 {len(products)} 個 GEMS 產品...")
        self._reset_stats()
        self.download_stats['start_time'] = time.time()

        downloaded_files: List[str] = []

        progress = make_download_progress(disable=not show_progress)

        with progress:
            for product in products:
                name = product.get('Name', 'unknown')
                date_param = product.get('date')
                product_code = product.get('ProductType', getattr(self, 'product_type', 'GEMS'))
                ver = product.get('ver', getattr(self, 'version', None))
                level = product.get('level', getattr(self, 'level', 'L2'))

                if not date_param:
                    self.logger.error(f"缺少下載時間戳，跳過: {name}")
                    self.download_stats['failed'] += 1
                    continue

                # 目錄結構 raw/{product}/{YYYY}/{MM}/
                year_month = f"{date_param[:4]}/{date_param[4:6]}"
                output_dir = self.raw_dir / product_code / year_month
                output_dir.mkdir(parents=True, exist_ok=True)
                output_path = output_dir / name

                if output_path.exists() and output_path.stat().st_size > 0:
                    self.logger.info(f"檔案已存在，跳過: {name}")
                    self.download_stats['skipped'] += 1
                    downloaded_files.append(str(output_path))
                    continue

                url = self._api_url(product_code, level, 'getFileItem.do')
                params = {'date': date_param, 'ver': ver, 'format': 'json', 'key': self.api_key}

                try:
                    with self.client.get(url, params=params, stream=True, timeout=600) as r:
                        ctype = r.headers.get('Content-Type', '')
                        if 'json' in ctype or 'xml' in ctype:
                            # 錯誤回應（非二進位檔）
                            raise RuntimeError(f"下載失敗：{r.text[:200]}")
                        total = int(r.headers.get('Content-Length', 0))
                        task = progress.add_task(f"[cyan]{name[:40]}", total=total or None)
                        written = self._stream_response_to_file(r, output_path, progress, task)

                    self.download_stats['success'] += 1
                    self.download_stats['actual_download_size'] += written
                    self.download_stats['total_size'] += written
                    downloaded_files.append(str(output_path))
                    self.logger.info(f"成功下載: {name} ({written / 1024 / 1024:.1f} MB)")

                except Exception as e:
                    self.logger.error(f"下載失敗: {name}, 錯誤: {e}")
                    self.download_stats['failed'] += 1

        self.logger.info(
            f"下載完成: 成功 {self.download_stats['success']}, "
            f"失敗 {self.download_stats['failed']}, 跳過 {self.download_stats['skipped']}, "
            f"共 {self.download_stats['actual_download_size'] / 1024 / 1024:.1f} MB"
        )
        return downloaded_files

    # ------------------------------------------------------------------ #
    # Streaming pipeline (download → grid → optionally delete raw)
    # ------------------------------------------------------------------ #
    def _download_granule(self, product: Dict[str, Any],
                          extract_bbox: Optional[tuple] = None) -> tuple[str, Optional[Path]]:
        """下載單一 granule。回傳 (status, path)：
            'ok'    下載成功，path 為原始檔
            'empty' server 回報範圍內無資料（區域裁切時）→ 不需重試
            'error' 下載失敗

        extract_bbox=(lon_min, lon_max, lat_min, lat_max)：用 getExtractFileItem.do 只取
            該區域（檔案小很多）；None 則用 getFileItem.do 取整盤 granule。
        """
        self._require_client()
        product_code = product['ProductType']
        ver, level = product.get('ver', getattr(self, 'version', None)), product.get('level', 'L2')
        date, name = product['date'], product['Name']

        out_dir = self.raw_dir / product_code / f"{date[:4]}/{date[4:6]}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / name
        if out_path.exists() and out_path.stat().st_size > 0:
            return 'ok', out_path

        if extract_bbox is not None:
            lon_min, lon_max, lat_min, lat_max = extract_bbox
            url = self._api_url(product_code, level, 'getExtractFileItem.do')
            # 注意：區域裁切「不可」帶 format 參數（帶了會 406）
            params = {'date': date, 'ver': ver, 'key': self.api_key,
                      'lt_lon': lon_min, 'lt_lat': lat_max,   # 左上 = 最小經度, 最大緯度
                      'rb_lon': lon_max, 'rb_lat': lat_min}   # 右下 = 最大經度, 最小緯度
        else:
            url = self._api_url(product_code, level, 'getFileItem.do')
            params = {'date': date, 'ver': ver, 'format': 'json', 'key': self.api_key}

        try:
            with self.client.get(url, params=params, stream=True, timeout=600) as r:
                ctype = r.headers.get('Content-Type', '')
                if 'netcdf' not in ctype and 'octet-stream' not in ctype:
                    body = r.text[:300]
                    if '"errorCode":3' in body or '데이터가 없' in body or 'No data' in body:
                        return 'empty', None
                    self.logger.warning(f"下載非預期回應 {name}: {body[:150]}")
                    return 'error', None
                self._stream_response_to_file(r, out_path)
            return 'ok', out_path
        except Exception as e:
            self.logger.error(f"下載失敗 {name}: {e}")
            return 'error', None

    def _mark_empty(self, product: Dict[str, Any]):
        """寫一個 .empty 標記，讓 resume 跳過「範圍內無資料」的時次（避免重抓）。"""
        date, name, code = product['date'], product['Name'], product['ProductType']
        base = self.processed_dir / code / f"{date[:4]}/{date[4:6]}"
        base.mkdir(parents=True, exist_ok=True)
        (base / (name + '.empty')).touch()

    def run_pipeline(self,
                     product_type: str,
                     start_date: str | datetime,
                     end_date: str | datetime,
                     ver: Optional[str] = None,
                     level: str = 'L2',
                     keep_raw: bool = False,
                     make_figures: bool = True,
                     skip_existing: bool = True,
                     extract_bbox: Optional[tuple] = None,
                     max_workers: int = 1,
                     limit: Optional[int] = None) -> Dict[str, Any]:
        """串流式管線：逐檔「下載 → 網格化(+繪圖) → 刪除原始」。

        峰值磁碟只佔「少數 granule + 累積的網格化小檔」，適合大量歷史回補。

        extract_bbox=(lon_min, lon_max, lat_min, lat_max)：用 server 端區域裁切（推薦），
            每檔由 ~270MB 降到 ~數MB；None 則下載整盤 granule。
        max_workers>1：並發下載（瓶頸是 server 端裁切 ~2.8s/檔），網格化(HDF5)仍在主執行緒
            序列化處理以策安全。建議 3~5（對政府 API 保守、避免被限流）。
        keep_raw=True 保留原始檔；False（預設）網格化後即刪。
        skip_existing=True（預設）：已有網格化輸出或 .empty 標記的時次直接跳過 → 可斷點續傳。
        """
        products = self.fetch_data(product_type, start_date, end_date,
                                   ver=ver, level=level, limit=limit)
        zero = {'processed': 0, 'empty': 0, 'raw_deleted': 0, 'raw_kept': 0, 'failed': 0, 'skipped': 0}
        if not products:
            return zero

        proc = self.processor
        months: set[tuple[str, str]] = set()
        c = {'ok': 0, 'empty': 0, 'del': 0, 'kept': 0, 'fail': 0, 'skip': 0}

        # 1) 斷點續傳：濾掉已完成(.nc)或已標記(.empty)的時次
        todo = []
        for product in products:
            name, date = product.get('Name', '?'), product.get('date')
            if skip_existing and date:
                base = self.processed_dir / proc.file_type / date[:4] / date[4:6]
                if (base / name).exists() or (base / (name + '.empty')).exists():
                    c['skip'] += 1
                    months.add((date[:4], date[4:6]))
                    continue
            todo.append(product)

        # 2) 處理單筆下載結果（只在主執行緒呼叫 → 網格化/HDF5 序列化、執行緒安全）
        def handle(product, status, raw_path):
            date = product.get('date')
            if status == 'empty':
                self._mark_empty(product)
                c['empty'] += 1
                months.add((date[:4], date[4:6]))
                return
            if status != 'ok' or raw_path is None:
                c['fail'] += 1
                return
            pstatus = proc.process_one(raw_path, make_figure=make_figures)
            if pstatus == 'ok':
                c['ok'] += 1
                months.add((date[:4], date[4:6]))
            elif pstatus == 'empty':
                self._mark_empty(product)
                c['empty'] += 1
            if pstatus == 'error':
                self.logger.warning(f"網格化失敗，保留原始檔以便檢查: {raw_path.name}")
                c['kept'] += 1
                c['fail'] += 1
            elif keep_raw:
                c['kept'] += 1
            else:
                try:
                    raw_path.unlink()
                    c['del'] += 1
                except OSError as e:
                    self.logger.warning(f"刪除原始檔失敗 {raw_path.name}: {e}")
                    c['kept'] += 1

        # 3) 下載（並發或序列），結果在主執行緒逐一 handle
        n = len(todo)
        if max_workers and max_workers > 1:
            from concurrent.futures import ThreadPoolExecutor, as_completed
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futs = {ex.submit(self._download_granule, p, extract_bbox): p for p in todo}
                for i, fut in enumerate(as_completed(futs), 1):
                    product = futs[fut]
                    try:
                        status, raw_path = fut.result()
                    except Exception as e:
                        self.logger.error(f"下載例外 {product.get('Name')}: {e}")
                        c['fail'] += 1
                        continue
                    self.logger.info(f"[{i}/{n}] {product.get('Name')}")
                    handle(product, status, raw_path)
        else:
            for i, product in enumerate(todo, 1):
                self.logger.info(f"[{i}/{n}] {product.get('Name')}")
                status, raw_path = self._download_granule(product, extract_bbox=extract_bbox)
                handle(product, status, raw_path)

        if make_figures:
            for y, m in sorted(months):
                proc.animate_month(y, m)

        self.logger.info(
            f"Pipeline 完成：網格化 {c['ok']}，空範圍 {c['empty']}，刪除原始 {c['del']}，"
            f"保留原始 {c['kept']}，失敗 {c['fail']}，跳過 {c['skip']}（keep_raw={keep_raw}, workers={max_workers}）"
        )
        return {'processed': c['ok'], 'empty': c['empty'], 'raw_deleted': c['del'],
                'raw_kept': c['kept'], 'failed': c['fail'], 'skipped': c['skip']}

    # ------------------------------------------------------------------ #
    # Processing
    # ------------------------------------------------------------------ #
    @property
    def processor(self):
        """延遲建立並回傳 GEMSProcessor（網格化＋繪圖）。"""
        if self._processor is None:
            if not hasattr(self, 'product_type'):
                raise ValueError("未設置 product_type，請先呼叫 fetch_data")
            from src.processing import GEMSProcessor
            # 用對應的 NESC 產品代碼（如 AOD→AERAOD）作為 file_type，與 raw 目錄一致
            file_type = self._resolve_product_code(self.product_type)
            self._processor = GEMSProcessor(file_type=file_type)
            self._processor.raw_dir = self.raw_dir
            self._processor.processed_dir = self.processed_dir
            self._processor.figure_dir = self.figure_dir
            self._processor.logger = self.logger
        return self._processor

    def process_data(self, pattern: Optional[str] = None,
                     start_date: Optional[datetime] = None,
                     end_date: Optional[datetime] = None,
                     make_figures: bool = True):
        """將已下載的 GEMS swath 網格化為標準 NetCDF 並繪圖。"""
        if not hasattr(self, 'product_type'):
            raise ValueError("未設置 product_type，請先呼叫 fetch_data")
        if start_date is None:
            start_date = getattr(self, 'start_date', None)
        if end_date is None:
            end_date = getattr(self, 'end_date', None)
        return self.processor.process_all_files(
            pattern=pattern, start_date=start_date, end_date=end_date,
            make_figures=make_figures,
        )

    def get_available_products(self) -> List[str]:
        return list(self.PRODUCT_TYPES.keys())

    def get_product_info(self, product_type: str) -> Dict[str, Any]:
        product_descriptions = {
            'NO2': {'name': '二氧化氮', 'description': '對流層/總量二氧化氮柱密度', 'unit': 'molec/cm²'},
            'O3T': {'name': '臭氧總量', 'description': '臭氧總量', 'unit': 'DU'},
            'O3P': {'name': '臭氧垂直分布', 'description': '臭氧 profile', 'unit': 'DU'},
            'HCHO': {'name': '甲醛', 'description': '甲醛柱密度', 'unit': 'molec/cm²'},
            'CHOCHO': {'name': '乙二醛', 'description': '乙二醛柱密度', 'unit': 'molec/cm²'},
            'SO2': {'name': '二氧化硫', 'description': '二氧化硫柱密度', 'unit': 'DU'},
            'AERAOD': {'name': '氣溶膠光學厚度', 'description': '氣溶膠光學厚度', 'unit': '無量綱'},
            'UVI': {'name': '紫外線指數', 'description': '地表紫外線指數', 'unit': '無量綱'},
        }
        code = self._resolve_product_code(product_type)
        return product_descriptions.get(code, {
            'name': code, 'description': 'GEMS 衛星數據產品', 'unit': '未知',
        }) | {'satellite': 'GEMS (GK-2B)', 'temporal_resolution': '1 小時 (白天)', 'resolution': '~3.5 x 8 km'}


if __name__ == '__main__':
    from datetime import timedelta

    gems = GEMSHub()
    products = gems.fetch_data(
        product_type='NO2',
        start_date='2023-05-15',
        end_date='2023-05-15',
        limit=3,
    )
    if products:
        gems.download_data(products[:1])
