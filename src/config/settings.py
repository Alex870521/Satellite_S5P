"""API 設定和常數"""
import os
import certifi
from pathlib import Path
from dotenv import load_dotenv


load_dotenv()
os.environ['SSL_CERT_FILE'] = certifi.where()

# API URLs
COPERNICUS_TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
COPERNICUS_BASE_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1"
COPERNICUS_DOWNLOAD_URL = "https://zipper.dataspace.copernicus.eu/odata/v1/Products"

# HTTP 設定
RETRY_SETTINGS = {
    'total': 5,
    'backoff_factor': 2,
    'status_forcelist': [429, 500, 502, 503, 504]
}

CHUNK_SIZE = 8192
DEFAULT_TIMEOUT = 60
DOWNLOAD_TIMEOUT = 180

# 存儲路徑：可用環境變數 SATELLITE_BASE_DIR 覆寫（建議寫在 .env）。
# 預設沿用外接碟；換機器只要設 SATELLITE_BASE_DIR，不需改碼。
BASE_DIR = Path(os.getenv("SATELLITE_BASE_DIR", "/Volumes/Transcend"))


# 地理範圍設定 (min_lon, max_lon, min_lat, max_lat)
FILTER_BOUNDARY = (120, 122, 22, 25)  # (118, 124, 20, 27)
# Taiwan regional boundary
FIGURE_BOUNDARY = (119, 123, 21, 26)  # (100, 145, 0, 45)

# 數據保留天數設定
# 在使用pipeline下，超過這個天數的檔案將被自動清理
DATA_RETENTION_DAYS = 30  # 預設保留30天

# 繪圖 DPI 設定（單一真相 / 集中管理出圖解析度）
# 各繪圖模組以 rcParams 套用這兩個值，不再於 plt.figure / savefig 個別硬寫 dpi。
FIGURE_DPI = 300  # 建立 figure（畫布）時的解析度
SAVE_DPI = 600    # savefig 輸出檔的解析度

# ERA5 相關配置
ERA5_STATIONS = [
    {"name": "FS", "lat": 22.6294, "lon": 120.3461},  # Kaohsiung Fengshan
    {"name": "NZ", "lat": 22.7422, "lon": 120.3339},  # Kaohsiung Nanzi
    {"name": "TH", "lat": 24.1817, "lon": 120.5956},  # Taichung
    {"name": "TP", "lat": 25.0330, "lon": 121.5654}   # Taipei
]

""" I/O structure
Main Folder (Sentinel_data)
├── logs
│   └── Satellite_S5P_202411.log
├── raw
│   ├── NO2___
│   │   ├── 2023
│   │   │   ├── 01
│   │   │   └── ...
│   │   └── 2024
│   │       ├── 01
│   │       └── ...
│   ├── SO2___
│   │   ├── 2023
│   │   │   ├── 01
│   │   │   └── ...
│   │   └── 2024
│   │       ├── 01
│   │       └── ...
│   └── ...
├── processed
│   ├── NO2___
│   │   ├── 2023
│   │   │   ├── 01
│   │   │   └── ...
│   │   └── 2024
│   │       ├── 01
│   │       └── ...
│   ├── SO2___
│   │   ├── 2023
│   │   │   ├── 01
│   │   │   └── ...
│   │   └── 2024
│   │       ├── 01
│   │       └── ...
│   └── ...
└── figure
    ├── NO2___
    │   ├── 2023
    │   │   ├── 01
    │   │   └── ...
    │   └── 2024
    │       ├── 01
    │       └── ...
    ├── SO2___
    │   ├── 2023
    │   │   ├── 01
    │   │   └── ...
    │   └── 2024
    │       ├── 01
    │       └── ...
    └── ...
"""