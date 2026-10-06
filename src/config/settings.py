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

# 存儲路徑：一律由環境變數 SATELLITE_BASE_DIR 決定（建議寫在 .env）。
# 預設是 repo 底下的 ./data，好讓剛 clone 的人跑得起來；實務上資料量是 TB 級，
# 請指到外接碟。⚠️ 以前這裡預設某顆外接碟的掛載點，那顆沒插時每個 hub 的
# constructor 都會在建資料夾時就炸掉，而且換機器的人完全看不懂為什麼。
BASE_DIR = Path(os.getenv("SATELLITE_BASE_DIR", Path(__file__).resolve().parents[2] / "data"))

# 資料可能散在多顆碟(例如舊年份留在封存碟、新年份寫在工作碟)。
# SATELLITE_DATA_ROOTS 用 os.pathsep 分隔,例如 "/Volumes/A:/Volumes/B";未設就只有 BASE_DIR。
# 需要「跨碟找同一個產品」的程式(l3_regrid_year、吃真實資料的測試)請用這個,不要寫死掛載點。
# 空字串視同未設(否則會得到 [],l3_regrid_year 就以為一顆碟都沒有)
DATA_ROOTS = [Path(x) for x in (os.getenv("SATELLITE_DATA_ROOTS") or str(BASE_DIR)).split(os.pathsep) if x]


# 地理範圍設定 (min_lon, max_lon, min_lat, max_lat)
FILTER_BOUNDARY = (120, 122, 22, 25)  # (118, 124, 20, 27)
# Taiwan regional boundary
FIGURE_BOUNDARY = (119, 123, 21, 26)  # (100, 145, 0, 45)

# 處理時的格網裁切區域 (GridFrame bounds: lon_min, lon_max, lat_min, lat_max)。
# 'taiwan' == GridFrame 預設台灣框(維持原樣,byte-identical)。新增的區域會輸出到
# 各衛星的 processed_<region>/ 資料夾,web 端會自動偵測成「區域」選項。
# 解析度不在這裡設定 —— 沿用各產品原本的 self.resolution(維持原解析度)。
REGIONS = {
    'taiwan': (118, 124, 20, 27),
    'east_asia': (100, 150, 0, 50),
}

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