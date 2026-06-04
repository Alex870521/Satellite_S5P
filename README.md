## <div align="center">Satellite Data Processing Toolkit</div>

<div align="center">

![Python](https://img.shields.io/badge/Python-3.12%20%7C%203.13%20%7C%203.14-blue.svg)
[![Tests](https://github.com/Alex870521/Satellite_S5P/actions/workflows/pytest.yml/badge.svg)](https://github.com/Alex870521/Satellite_S5P/actions/workflows/pytest.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
![GitHub last commit](https://img.shields.io/github/last-commit/Alex870521/Satellite_S5P?logo=github)

</div>

---

A comprehensive Python toolkit for retrieving, processing, and visualizing satellite data from multiple sources: Sentinel-5P, MODIS, ERA5, and GEMS. This toolkit focuses on atmospheric data including air pollutants (NO₂, CO, SO₂, O₃, HCHO), aerosol optical depth, and meteorological parameters.

## <div align="center">Features</div>

- **Multi-platform Support**:
  - **Sentinel-5P**: Trace gases and air pollutants
  - **MODIS**: Aerosol optical depth (AOD) measurements
  - **ERA5**: Reanalysis of atmospheric, land, and oceanic climate variables
  - **GEMS**: Geostationary hourly trace gases & aerosol over East Asia (daytime)

- **Unified Data Access**:
  - Automated data retrieval from Copernicus Open Access Hub, NASA Earthdata, Climate Data Store, and the NIER/NESC GEMS Open-API
  - Consistent API across different data sources

- **Advanced Processing**:
  - Quality control and filtering
  - Spatial interpolation and regridding
  - Temporal aggregation
  - Station-based data extraction

- **Visualization**:
  - High-quality concentration and parameter maps
  - Customizable geographic boundaries
  - Time series analysis capabilities

- **Resource Management**:
  - Efficient download handling with caching
  - Built-in file retention management

## <div align="center">Prerequisites</div>

Before using this toolkit, you need to complete the following steps:

1. **Copernicus Account** (for Sentinel-5P and ERA5):
   - Register for a free account at [Copernicus Open Access Hub](https://scihub.copernicus.eu/dhus/#/home)
   - For ERA5, also register at [Climate Data Store](https://cds.climate.copernicus.eu/)

2. **NASA Earthdata Account** (for MODIS):
   - Register at [NASA Earthdata](https://urs.earthdata.nasa.gov/)

3. **GEMS Open-API Key** (for GEMS):
   - Request a key at [NIER/NESC](https://nesc.nier.go.kr) and use the "single key" mode

4. **Environment Configuration**:
   - Create a `.env` file in the project root directory with your credentials:
     ```
     # Sentinel-5P credentials
     COPERNICUS_USERNAME=your_username
     COPERNICUS_PASSWORD=your_password
     
     # ERA5 credentials
     CDSAPI_URL=https://cds.climate.copernicus.eu/api/v2
     CDSAPI_KEY=your_key
     
     # NASA Earthdata credentials
     EARTHDATA_USERNAME=your_username
     EARTHDATA_PASSWORD=your_password

     # GEMS credentials (request a key at https://nesc.nier.go.kr)
     GEMS_API_KEY=your_key

     # Where downloads/outputs are stored. REQUIRED unless the default
     # external drive (/Volumes/Transcend) is mounted — otherwise creating a
     # hub (e.g. SENTINEL5PHub()) fails immediately while making its data dirs.
     SATELLITE_BASE_DIR=/path/to/your/data
     ```

> [!IMPORTANT]
> **`SATELLITE_BASE_DIR` controls where data is written.** It defaults to
> `/Volumes/Transcend`; if that drive is not mounted, every hub constructor
> raises a `PermissionError`/`FileNotFoundError` before it can do anything.
> Set it to a local path (e.g. `./data`) on any machine without that drive.
>
> **NASA Earthdata `Token does not exist`:** if MODIS search/download fails
> with `{"errors":["Token does not exist"]}`, a stale bearer token on the
> `cmr.earthdata.nasa.gov` line of your `~/.netrc` is being sent by
> `python-cmr`. Remove that line (or regenerate the token at
> [urs.earthdata.nasa.gov](https://urs.earthdata.nasa.gov/)).

## <div align="center">Installation</div>

```bash
# Clone the repository
git clone https://github.com/Alex870521/Satellite_S5P.git
cd Satellite_S5P

# Core install — downstream analysis & visualization (reads NetCDF only)
pip install .

# ...or add the HDF4 ingest extra, only if you need to convert raw MODIS .hdf files
pip install ".[ingest]"
```

> [!IMPORTANT]
> **Reading raw MODIS HDF4 (`.hdf`) requires `pyhdf`**, which ships only in the
> optional `[ingest]` extra. The core package and all downstream processing read
> **NetCDF only** and do not need `pyhdf` — raw `.hdf` is converted to `.nc` once
> at ingest, and everything else (plots, FNR, analysis) reads the NetCDF output.
>
> | What you do | Recommended Python | Needs `pyhdf`? |
> |---|---|---|
> | Downstream analysis (read `.nc`, plots, FNR) | 3.12 / 3.13 / **3.14** | No — `pip install .` |
> | Convert raw `.hdf` → `.nc` (ingest) | **3.12 / 3.13** | Yes — `pip install ".[ingest]"` |
>
> ⚠️ **Python 3.14 + HDF4 ingest is not supported yet.** `pyhdf` has no 3.14 wheel
> on any platform (including Windows `win_amd64`), so `pip install ".[ingest]"`
> falls back to a source build and fails (missing HDF4 headers). Run the ingest /
> conversion step under Python 3.12 or 3.13; keep 3.14 for analysis only.
>
> 🪟 **Windows users:** `pyhdf` provides wheels for Python 3.7–3.13 (`win_amd64`),
> so `pip install ".[ingest]"` works out of the box on 3.12/3.13 — no conda or
> manual HDF4 setup needed. On **Windows + Python 3.14**, run ingest under
> 3.12/3.13 (or `conda install -c conda-forge pyhdf`); use 3.14 for analysis only.

For a pinned development environment you can still use `pip install -r requirements.txt`
(note: `pyhdf` there is ingest-only — see the comment in the file).

## <div align="center">Usage</div>

Every hub exposes a single one-call **`run_pipeline()`** that runs *fetch → download → process*.
The three steps (`fetch_data` / `download_data` / `process_data`) remain available when you need to
inspect or control each stage. Query parameters differ per source (forwarded to `fetch_data`).

### Sentinel-5P Example

```python
"""SENTINEL-5P Data Processing Example"""
from datetime import datetime
from src.api import SENTINEL5PHub

# file_class: 'NRTI' (Near Real-Time) or 'OFFL' (Offline processed)
# file_type:  'NO2___', 'O3____', 'CO____', 'SO2___', 'CH4___', 'CLOUD_', 'AER_AI'
sentinel_hub = SENTINEL5PHub(max_workers=3)
sentinel_hub.run_pipeline(
    file_class='NRTI',
    file_type='NO2___',
    start_date=datetime(2025, 3, 1),
    end_date=datetime(2025, 3, 13),
    boundary=(120, 122, 22, 25),   # (min_lon, max_lon, min_lat, max_lat)
)
```

### MODIS Example

```python
"""MODIS Data Processing Example"""
from datetime import datetime
from src.api import MODISHub

# Product types: 'MOD04_L2' (Terra) / 'MYD04_L2' (Aqua) / 'MCD19A2' (MAIAC)
modis_hub = MODISHub()
modis_hub.run_pipeline(
    file_type='MYD04_L2',
    start_date=datetime(2025, 3, 1),
    end_date=datetime(2025, 3, 12),
)
```

### ERA5 Example

```python
"""ERA5 Data Processing Example"""
from datetime import datetime
from src.api import ERA5Hub

# Observation stations extracted to CSV (ERA5 outputs CSV, not maps)
STATIONS = [
    {"name": "FS", "lat": 22.6294, "lon": 120.3461},  # Kaohsiung Fengshan
    {"name": "NZ", "lat": 22.7422, "lon": 120.3339},  # Kaohsiung Nanzi
    {"name": "TH", "lat": 24.1817, "lon": 120.5956},  # Taichung
    {"name": "TP", "lat": 25.0330, "lon": 121.5654},  # Taipei
]

era5_hub = ERA5Hub(timezone='Asia/Taipei')
era5_hub.run_pipeline(
    start_date=datetime(2025, 3, 1),
    end_date=datetime(2025, 3, 19),
    boundary=(119, 123, 21, 26),         # (min_lon, max_lon, min_lat, max_lat)
    variables=['boundary_layer_height'],
    pressure_levels=None,                # None = surface data only
    download_mode='all_at_once',         # or 'monthly'
    stations=STATIONS,                   # required, else nothing is written to CSV
)
```

> [!NOTE]
> Unlike Sentinel-5P / MODIS / GEMS, the ERA5 pipeline does **not** render maps —
> it extracts each station's time series to CSV (pass `stations=...`).

### GEMS Example

```python
"""GEMS Data Processing Example"""
from src.api import GEMSHub

# Product types: 'NO2', 'O3'/'O3T', 'O3P', 'SO2', 'HCHO', 'CHOCHO',
#                'AOD'/'AERAOD', 'AEH', 'UVI', 'CLOUD'
gems_hub = GEMSHub()   # requires GEMS_API_KEY in .env
gems_hub.run_pipeline(
    product_type='NO2',
    start_date='2023-05-15',
    end_date='2023-05-15',
    ver=None,                            # None = resolve the latest version online (NO2 v4.0.1)
    extract_bbox=(119, 123, 21, 26),     # server-side Taiwan crop: ~270 MB -> ~2-3 MB / granule
    max_workers=3,                       # concurrent downloads
    skip_existing=True,                  # resumable backfill
)
```

> [!TIP]
> GEMS `run_pipeline` streams *download → grid → delete-raw* per granule, so peak disk stays at
> a few MB with `extract_bbox`. Pass `keep_raw=True` to retain raw swaths, or `make_figures=False`
> to skip plotting during large backfills.

## <div align="center">Data Sources</div>

### Sentinel-5P
- **Provider**: European Space Agency (ESA) — Copernicus / TROPOMI
- **Frequency**: daily global coverage; processing classes `NRTI` / `OFFL` / `RPRO`
- **Auth**: `COPERNICUS_USERNAME` / `COPERNICUS_PASSWORD` (Copernicus Data Space)

| Product | `file_type` | Resolution (km) | Quantity |
|---------|-------------|-----------------|----------|
| NO₂ | `NO2___` | 5.5 × 3.5 | Tropospheric column |
| O₃ | `O3____` | 5.5 × 3.5 | Total vertical column |
| O₃ profile | `O3__PR` | 30 × 30 | Vertical profile |
| SO₂ | `SO2___` | 5.5 × 3.5 | Total vertical column |
| HCHO | `HCHO__` | 5.5 × 3.5 | Tropospheric vertical column |
| CO | `CO____` | 5.5 × 7 | Total column |
| CH₄ | `CH4___` | 5.5 × 7 | Column-averaged mixing ratio |
| Aerosol Index | `AER_AI` | 5.5 × 3.5 | UV aerosol index |
| Cloud | `CLOUD_` | 5.5 × 3.5 | Cloud fraction / properties |

> Nadir resolution is 5.5 × 3.5 km since 2019-08-06 (7 × 3.5 km before).

### MODIS
- **Provider**: NASA — Terra & Aqua (Dark Target) / combined (MAIAC)
- **Frequency**: 1–2 days global coverage
- **Auth**: `EARTHDATA_USERNAME` / `EARTHDATA_PASSWORD` (NASA Earthdata)

| Product | Platform | Algorithm (level) | Resolution |
|---------|----------|-------------------|------------|
| `MOD04_L2` / `MYD04_L2` | Terra / Aqua | Dark Target AOD (L2) | 10 km |
| `MOD04_3K` / `MYD04_3K` | Terra / Aqua | Dark Target AOD (L2) | 3 km |
| `MCD19A2` | Terra + Aqua | MAIAC AOD (L3) | 1 km |

### ERA5
- **Provider**: European Centre for Medium-Range Weather Forecasts (ECMWF)
- **Frequency**: hourly, monthly updates; 0.25° × 0.25° global grid (~31 km)
- **Auth**: `CDSAPI_URL` / `CDSAPI_KEY` (Climate Data Store)

| Type | Examples | Levels |
|------|----------|--------|
| Single-level (surface) | boundary_layer_height, 2 m temperature, 10 m wind, mean sea-level pressure | surface |
| Pressure-level | temperature, u/v wind, geopotential, relative humidity | 37 levels (1000–1 hPa) |

> This toolkit extracts per-station time series to CSV (no maps); 100+ variables are available.

### GEMS
- **Provider**: NIER Environmental Satellite Center (NESC), Korea — GK-2B geostationary
- **Frequency**: hourly, **daytime only**, over East/South-East Asia (up to ~10 scans/day)
- **Auth**: set `GEMS_API_KEY` in `.env` (request a key at [nesc.nier.go.kr](https://nesc.nier.go.kr))

| Level | Products | Spatial resolution | Coverage (UTC) |
|-------|----------|--------------------|----------------|
| **L2** (swath) | NO₂, O₃ (O3T), SO₂, HCHO, CHOCHO, Aerosol (AOD/AEH), UVI, Cloud | **3.5 km × 8 km** (N–S × E–W) at Seoul; ≈ 2.8 × 7.1 km measured over Taiwan. CHOCHO is co-added (coarser) | trace gases since **~2020-09**, ongoing |
| **L3** (gridded) | NO₂ daily / monthly mean — column & tropospheric (whole domain / KR / EA) | **~5 km over Korea, ~10 km elsewhere** | since **~2020-09**, ongoing |
| **L4** (surface) | Surface PM₂.₅, PM₁₀, NO₂ | gridded surface product | since **~2021-12**, ongoing |

> Native L2 pixel size grows toward the scan edges. This toolkit re-grids L2 swaths onto a regular
> grid (default **8 km × 3.5 km**, set per-product in `GEMSProcessor`) before plotting/export.

## <div align="center">Documentation</div>

Per-source and topic guides live under [`docs/`](docs/):

- [GEMS API](docs/GEMS_API_README.md) — products, usage, storage layout, `GEMS_API_KEY` setup
- [Himawari API](docs/Himawari_API_README.md) — products & usage *(mock; not yet wired to a real service)*
- [MODIS AOD variables](docs/MODIS_AOD_Variables_README.md) — AOD variable reference
- [MODIS HDF → NetCDF merge](docs/MODIS_HDF_Merge_README.md) — raw `.hdf` ingest/merge notes

## <div align="center">Processing Pipeline</div>

All data sources follow a consistent workflow:

1. **Data Discovery**: Query available products based on date range and region
2. **Download Management**: Efficient parallel downloading with error handling
3. **Quality Control**: Filtering based on data quality flags
4. **Spatial Processing**: 
   - Sentinel-5P: RBF interpolation of sparse satellite data
   - MODIS: Processing of gridded AOD values
   - ERA5: Extraction of point values for weather stations
5. **Visualization**: Generation of standardized maps and plots
6. **Export**: Structured data storage in NetCDF and CSV formats

## <div align="center">Storage Layout</div>

All downloads and outputs live under a single **base directory**, configured once via the
`SATELLITE_BASE_DIR` environment variable in `.env` (falls back to `/Volumes/Transcend` if unset).
Every satellite hub then builds the same `{base}/{Satellite}/{raw|processed|figure}/...` tree.

Anatomy of a downloaded file path (GEMS NO₂ example):

```
$SATELLITE_BASE_DIR / GEMS / raw /  NO2  / 2023/05 / GK2_GEMS_L2_..._NO2_..._.nc
└───── ① base ─────┘  └─②─┘ └─③─┘ └─④─┘ └──⑤──┘  └──────────── ⑥ ───────────┘
```

| # | Segment | Value (example) | Where it is set |
|---|---------|-----------------|-----------------|
| ① | **base dir** | `…/DataCenter/Satellite` | `.env` → `SATELLITE_BASE_DIR` (read by `src/config/settings.py` → `BASE_DIR`) |
| ② | **satellite** | `GEMS` | each hub's `name` attribute → `core.py` `main_dir = base_dir / name` |
| ③ | **stage** | `raw` (also `processed`, `figure`, `logs`) | `core.py` `_setup_common_dirs()` |
| ④ | **product** | `NO2` | hub download step (e.g. `gems_api.py`) |
| ⑤ | **year/month** | `2023/05` | derived from each file's timestamp |
| ⑥ | **filename** | original granule name | from the data provider |

Resulting tree:

```
$SATELLITE_BASE_DIR/
├── Sentinel-5P/ { raw, processed, figure, geotiff, logs }/<product>/<YYYY>/<MM>/
├── MODIS/       { raw, processed, figure, logs }/<product>/<YYYY>/<MM>/
├── ERA5/        { raw, processed, figure, logs }/...
└── GEMS/        { raw, processed, figure, logs }/<product>/<YYYY>/<MM>/
    ├── raw/       NO2/2023/05/GK2_GEMS_L2_20230515_0345_NO2_..._.nc   ← downloaded swath
    ├── processed/ NO2/2023/05/GK2_GEMS_L2_20230515_0345_NO2_..._.nc   ← gridded NetCDF
    └── figure/    NO2/2023/05/GK2_GEMS_L2_20230515_0345_NO2_..._.png   ← map + monthly .gif
```

> **To relocate all data**, change only `SATELLITE_BASE_DIR` in `.env` — no code changes needed.

## <div align="center">Automatic Data Management</div>

The toolkit includes built-in data retention management to prevent disk space issues:

- Automatically cleans files older than the configured retention period
- Maintains directory structure while removing outdated files
- Can be scheduled for periodic execution or triggered manually

## <div align="center">Contact</div>

For bug reports and feature requests please visit [GitHub Issues](https://github.com/Alex870521/Satellite_DataKit/issues).