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

# 存儲路徑
BASE_DIR = Path("/Users/chanchihyu/DataCenter/Satellite")

# 地理範圍設定 (min_lon, max_lon, min_lat, max_lat)
FILTER_BOUNDARY = (120, 122, 22, 25)  # (118, 124, 20, 27)
FIGURE_BOUNDARY = (119, 123, 21, 26)  # (100, 145, 0, 45)

# 數據保留天數設定
# 在使用pipeline下，超過這個天數的檔案將被自動清理
DATA_RETENTION_DAYS = 30  # 預設保留30天


""" I/O structure
Main Folder (Sentinel_data)
├── figure
│   ├── NO2
│   │   ├── 2023
│   │   │   ├── 01
│   │   │   └── ...
│   │   └── 2024
│   │       ├── 01
│   │       └── ...
│   └── ...
├── logs
│   └── Satellite_S5P_202411.log
├── processed
│   ├── NO2
│   │   ├── 2023
│   │   │   ├── 01
│   │   │   └── ...
│   │   └── 2024
│   │       ├── 01
│   │       └── ...
│   └── ...
└── raw
    ├── NO2
    │   ├── 2023
    │   │   ├── 01
    │   │   └── ...
    │   └── 2024
    │       ├── 01
    │       └── ...
    └── ...
"""