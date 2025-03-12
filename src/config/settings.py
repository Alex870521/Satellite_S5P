"""API 設定和常數"""
from pathlib import Path
import os
import certifi
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

SENTINEL5P_DATA_BASE_DIR = BASE_DIR / 'Sentinel-5P'
MODIS_DATA_BASE_DIR = BASE_DIR / 'MODIS'

RAW_DATA_DIR = SENTINEL5P_DATA_BASE_DIR / "raw"
PROCESSED_DATA_DIR = SENTINEL5P_DATA_BASE_DIR / "processed"
GEOTIFF_DIR = SENTINEL5P_DATA_BASE_DIR / "geotiff"
FIGURE_DIR = SENTINEL5P_DATA_BASE_DIR / "figure"
LOGS_DIR = SENTINEL5P_DATA_BASE_DIR / "logs"

MODIS_RAW_DATA_DIR = MODIS_DATA_BASE_DIR / "raw"
MODIS_PROCESSED_DATA_DIR = MODIS_DATA_BASE_DIR / "processed"
MODIS_FIGURE_DIR = MODIS_DATA_BASE_DIR / "figure"
MODIS_LOGS_DIR = MODIS_DATA_BASE_DIR / "logs"

# 地理範圍設定
FILTER_BOUNDARY = (120, 122, 22, 25)  # (118, 124, 20, 27)
FIGURE_BOUNDARY = (119, 123, 21, 26)  # (100, 145, 0, 45)

# 數據保留天數設定
# 在使用pipeline下，超過這個天數的檔案將被自動清理
DATA_RETENTION_DAYS = 7  # 預設保留7天