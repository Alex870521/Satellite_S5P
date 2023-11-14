"""檔案下載處理"""
import os
import requests
import logging
from pathlib import Path
from tqdm import tqdm

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.config.settings import RETRY_SETTINGS, CHUNK_SIZE, DOWNLOAD_TIMEOUT

logger = logging.getLogger(__name__)


class Downloader:
    def __init__(self):
        self.session = requests.Session()
        self.session.trust_env = False
        self._retries = requests.adapters.Retry(
            total=5,
            backoff_factor=0.1,
            status_forcelist=[500, 502, 503, 504],
        )
        self.session.mount('https://', requests.adapters.HTTPAdapter(max_retries=self._retries))

    def download_file(self, url, headers, output_path, progress_callback=None):
        """下載檔案並更新進度

        Args:
            url: 下載URL
            headers: 請求標頭
            output_path: 輸出路徑
            progress_callback: 進度回調函數，接收已下載的字節數
        """
        try:
            # 使用 stream=True 來分塊下載
            response = self.session.get(url, headers=headers, stream=True)
            response.raise_for_status()

            # 獲取檔案總大小
            total_size = int(response.headers.get('content-length', 0))
            block_size = 8192
            downloaded = 0

            # 建立臨時檔案
            temp_path = output_path.with_suffix('.tmp')

            try:
                with open(temp_path, 'wb') as file:
                    for data in response.iter_content(block_size):
                        file.write(data)
                        downloaded += len(data)
                        if progress_callback:
                            progress_callback(min(downloaded, total_size))

                # 下載完成後，將臨時檔案重命名為正式檔案
                if temp_path.exists():
                    if output_path.exists():
                        output_path.unlink()
                    temp_path.rename(output_path)
                return True

            finally:
                # 清理臨時檔案
                if temp_path.exists():
                    temp_path.unlink()

        except requests.exceptions.RequestException as e:
            logger.error(f"Download error: {str(e)}")
            return False

        except Exception as e:
            logger.error(f"Unexpected error during download: {str(e)}")
            return False
