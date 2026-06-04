import json
import requests
import logging
import zipfile
from pathlib import Path
from requests.adapters import HTTPAdapter


logger = logging.getLogger(__name__)


class DownloadManifest:
    """追蹤下載進度，支援斷點續傳"""

    def __init__(self, manifest_path: Path):
        self.path = manifest_path
        self._data: dict = self._load()

    def _load(self) -> dict:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text())
            except (json.JSONDecodeError, OSError):
                return {}
        return {}

    def _save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self._data, indent=2))

    def is_complete(self, file_key: str) -> bool:
        return self._data.get(file_key, {}).get('status') == 'complete'

    def mark_downloading(self, file_key: str, url: str, output_path: str):
        self._data[file_key] = {
            'status': 'downloading',
            'url': url,
            'output_path': output_path,
        }
        self._save()

    def mark_complete(self, file_key: str):
        if file_key in self._data:
            self._data[file_key]['status'] = 'complete'
            self._save()

    def mark_failed(self, file_key: str, error: str):
        if file_key in self._data:
            self._data[file_key]['status'] = 'failed'
            self._data[file_key]['error'] = error
            self._save()

    def remove(self, file_key: str):
        self._data.pop(file_key, None)
        self._save()

    def get_incomplete(self) -> list[str]:
        return [k for k, v in self._data.items() if v.get('status') != 'complete']


class Downloader:
    def __init__(self, manifest_dir: Path = None):
        self.session = requests.Session()
        self.session.trust_env = False
        self._retries = requests.adapters.Retry(
            total=5,
            backoff_factor=10,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        self.session.mount('https://', requests.adapters.HTTPAdapter(max_retries=self._retries))

        # 下載清單（斷點續傳用）
        self.manifest = None
        if manifest_dir:
            self.manifest = DownloadManifest(manifest_dir / '.download_manifest.json')

    def download_data(self, url, headers, output_path, progress_callback=None, extract_zip=True):
        """下載檔案，支援斷點續傳。

        Args:
            url: 下載URL
            headers: 請求標頭
            output_path: 輸出路徑
            progress_callback: 進度回調函數，接收已下載的字節數
            extract_zip: True（預設，Copernicus）→ 下載的是 zip，完成後解壓出 .nc
                到 output_path；False（純串流，如 GEMS NetCDF）→ 直接把回應位元組
                串流寫到 output_path，不做任何 zip 處理。
        """
        file_key = str(output_path)

        # 檢查 manifest 是否已標記完成
        if self.manifest and self.manifest.is_complete(file_key):
            # zip 模式額外確認 output 不是殘留的 zip（代表解壓未完成）
            already_done = output_path.exists() and (not extract_zip or not zipfile.is_zipfile(output_path))
            if already_done:
                logger.debug(f"Manifest shows complete, skipping: {output_path.name}")
                return True

        if self.manifest:
            self.manifest.mark_downloading(file_key, url, str(output_path))

        try:
            if extract_zip:
                self._download_zip(url, headers, output_path, progress_callback)
            else:
                self._download_plain(url, headers, output_path, progress_callback)

            if self.manifest:
                self.manifest.mark_complete(file_key)
            return True

        except Exception as e:
            logger.error(f"Download error: {str(e)}")
            if self.manifest:
                self.manifest.mark_failed(file_key, str(e))
            return False

    def _stream_to_temp(self, url, headers, temp_path, progress_callback=None):
        """串流下載到 temp_path，支援 Range 斷點續傳，並回報 progress_callback。

        zip 與純串流兩種模式共用的 HTTP 串流核心。
        """
        downloaded = 0
        request_headers = dict(headers)

        if temp_path.exists():
            downloaded = temp_path.stat().st_size
            request_headers['Range'] = f'bytes={downloaded}-'
            logger.info(f"Resuming download from {downloaded} bytes: {temp_path.name}")

        response = self.session.get(url, headers=request_headers, stream=True)

        # 206 = partial content (resume), 200 = full content (server doesn't support Range)
        if response.status_code == 200 and downloaded > 0:
            downloaded = 0
            logger.debug(f"Server doesn't support Range, restarting: {temp_path.name}")

        response.raise_for_status()

        total_size = int(response.headers.get('content-length', 0)) + downloaded
        block_size = 8192

        mode = 'ab' if response.status_code == 206 else 'wb'
        with open(temp_path, mode) as file:
            for data in response.iter_content(block_size):
                file.write(data)
                downloaded += len(data)
                if progress_callback:
                    progress_callback(min(downloaded, total_size))

    def _download_zip(self, url, headers, output_path, progress_callback=None):
        """Copernicus 模式：下載 zip → 解壓第一個 .nc 到 output_path。"""
        temp_path = output_path.with_suffix('.tmp')
        zip_path = output_path.with_suffix('.zip')

        try:
            if output_path.exists() and zipfile.is_zipfile(output_path):
                output_path.rename(zip_path)

            elif zip_path.exists() and not zipfile.is_zipfile(zip_path):
                zip_path.rename(output_path)
                return

            elif zip_path.exists() and zipfile.is_zipfile(zip_path):
                pass  # zip 已存在，直接解壓

            else:
                self._stream_to_temp(url, headers, temp_path, progress_callback)
                temp_path.rename(zip_path)

            # 解壓縮處理
            if zipfile.is_zipfile(zip_path):
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    nc_files = [f for f in zip_ref.namelist() if f.endswith('.nc')]
                    if nc_files:
                        with zip_ref.open(nc_files[0]) as source, open(output_path, 'wb') as target:
                            target.write(source.read())
            else:
                zip_path.rename(output_path)

        finally:
            # 不刪除 temp 檔案（留給續傳用），只在成功後清理 zip
            if zip_path.exists():
                zip_path.unlink()

    def _download_plain(self, url, headers, output_path, progress_callback=None):
        """純串流模式：直接把回應位元組寫到 output_path，不做 zip 處理。

        先寫到 `<name>.part`（續傳用），完成後 atomic rename 到 output_path。
        """
        temp_path = output_path.with_suffix(output_path.suffix + '.part')
        self._stream_to_temp(url, headers, temp_path, progress_callback)
        temp_path.replace(output_path)
