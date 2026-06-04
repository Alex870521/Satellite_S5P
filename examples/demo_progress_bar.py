"""
下載進度條 Demo（無需網路 / 憑證）

直接呼叫真正的 SentinelHubBase.download_data，用假的 downloader 模擬分段下載，
讓你在真實終端機看到「固定 N 條 Thread 列、原地刷新」的進度條效果。

跑法：
    python examples/demo_progress_bar.py
或（在 Claude Code 輸入框）：
    ! .venv/bin/python examples/demo_progress_bar.py

可調參數見檔案最底下的 __main__ 區塊：
    N_PRODUCTS   下載幾個產品
    MAX_WORKERS  worker 列數（= 同時下載數）
    FAIL_IDS     指定哪幾個 index 故意下載失敗（看失敗→刪檔→統計）
    SPEED        每段傳輸延遲；調大跑慢一點、更容易觀察
"""
import sys
import time
import logging
import tempfile
import threading
from pathlib import Path

# 讓腳本從任何位置都能執行：把專案根目錄加進 import 路徑
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.api.sentinel_api import SentinelHubBase  # noqa: E402

# 真終端機：壓掉 info、只看 rich 進度條；
# 非互動（被 `!`/管線捕捉）：顯示逐檔文字狀態，讓你照樣看得到下載進度。
logging.basicConfig(
    level=logging.ERROR if sys.stdout.isatty() else logging.INFO,
    format="%(message)s",
)


class FakeDownloader:
    """模擬真實下載：分段寫入並回報 progress_callback，讓進度條會動。

    參數（檔案大小 / 每段延遲 / 是否失敗）透過 product 的 Id 編進 url 帶進來，
    這樣就能完全沿用真正的 download_data 流程，不需要改任何產品碼。
    """

    def download_data(self, url, headers, output_path, progress_callback=None):
        # download_url 形如 ".../Products(size=..&delay=..&fail=1)/$value"
        # 取括號內的 query 字串，再解析成 dict（避免被尾巴 ")/$value" 干擾）
        query = url.split("(", 1)[1].rsplit(")", 1)[0]
        params = dict(kv.split("=") for kv in query.split("&"))
        size = int(params["size"])
        delay = float(params["delay"])
        fail = params.get("fail") == "1"

        chunk = max(size // 40, 1)  # 分成 ~40 段
        downloaded = 0
        with open(output_path, "wb") as f:
            while downloaded < size:
                step = min(chunk, size - downloaded)
                f.write(b"\0" * step)
                downloaded += step
                if progress_callback:
                    progress_callback(downloaded)
                time.sleep(delay)
                # 故意失敗：下載到一半就放棄（回傳 False → download_data 會刪檔 + 計 failed）
                if fail and downloaded >= size // 2:
                    return False
        return True


def make_hub(max_workers):
    o = SentinelHubBase.__new__(SentinelHubBase)
    o.downloader = FakeDownloader()
    o.auth = type("FakeAuth", (), {"ensure_valid_token": lambda s: "demo-token"})()
    o._token_lock = threading.Lock()
    o.file_type = "NO2___"
    o.raw_dir = Path(tempfile.mkdtemp(prefix="demo_dl_"))
    o.logger = logging.getLogger("demo")
    o.max_workers = max_workers
    o.download_stats = {
        "success": 0, "failed": 0, "skipped": 0,
        "total_size": 0, "actual_download_size": 0,
    }
    return o


def make_products(n, fail_ids, speed):
    import random
    rng = random.Random(42)  # 固定種子，每次跑長相一致
    sizes_mb = [20, 45, 80, 120, 200, 350]
    products = []
    for i in range(n):
        size = rng.choice(sizes_mb) * 1024 * 1024
        fail_flag = "&fail=1" if i in fail_ids else ""
        products.append({
            "Name": f"S5P_OFFL_L2__NO2____2024010{i % 9}T{i:02d}0823_{i:05d}.nc",
            "ContentLength": size,
            # Id 會被組進 download_url，FakeDownloader 再從中解析這些參數
            "Id": f"size={size}&delay={speed}{fail_flag}",
            "ContentDate": {"Start": "2024-01-01T00:00:00.000000Z"},
        })
    return products


if __name__ == "__main__":
    # ---- 可調參數 ----
    N_PRODUCTS = 12
    MAX_WORKERS = 4
    FAIL_IDS = {5, 9}      # 這兩個故意失敗；設成 set() 則全部成功
    SPEED = 0.04           # 每段延遲(秒)；調大 = 跑慢一點更好觀察
    # ------------------

    print(f"\nDemo：{N_PRODUCTS} 個產品 / {MAX_WORKERS} 條 worker 列 / "
          f"故意失敗 index={sorted(FAIL_IDS) or '無'}\n")

    # 非互動終端機（被 `!`、管線或重導向捕捉）下，download_data 會自動退回
    # 「逐檔文字狀態」顯示，不畫 thread 進度條（避免在非 TTY 折行洗版）。
    if not sys.stdout.isatty():
        print("ℹ️ 非互動終端機：改以逐檔文字狀態顯示下載進度（不畫 thread 進度條）。\n"
              "   想看固定列原地刷新的進度條，請在真實終端機分頁直接執行本檔。\n")

    hub = make_hub(MAX_WORKERS)
    hub.download_data(make_products(N_PRODUCTS, FAIL_IDS, SPEED), show_progress=True)
