## <div align="center">Satellite Data Processing Toolkit</div>

<div align="center">

![Python](https://img.shields.io/badge/Python-3.12%20%7C%203.13%20%7C%203.14-blue.svg)
[![Tests](https://github.com/Alex870521/Satellite_S5P/actions/workflows/pytest.yml/badge.svg)](https://github.com/Alex870521/Satellite_S5P/actions/workflows/pytest.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
![GitHub last commit](https://img.shields.io/github/last-commit/Alex870521/Satellite_S5P?logo=github)

</div>

---

Download, grid and plot atmospheric satellite data — **Sentinel-5P**, **MODIS**, **GEMS** and **ERA5** —
through one API per source, and regrid all of them onto one common 0.02° grid.

## Installation

```bash
git clone https://github.com/Alex870521/Satellite_S5P.git
cd Satellite_S5P
pip install .               # analysis and plotting (reads NetCDF)
pip install ".[ingest]"     # + pyhdf, only to read raw MODIS .hdf
```

> [!NOTE]
> `pyhdf` has no Python 3.14 wheel yet: run MODIS `.hdf` ingest under 3.12/3.13 and use 3.14 for analysis only.

## Configuration

Create `.env` in the repo root:

```ini
COPERNICUS_USERNAME=...      # Sentinel-5P
COPERNICUS_PASSWORD=...
S3_ACCESS_KEY=...            # optional, much faster bulk S5P downloads
S3_SECRET_KEY=...
CDSAPI_URL=https://cds.climate.copernicus.eu/api   # ERA5
CDSAPI_KEY=...
EARTHDATA_USERNAME=...       # MODIS
EARTHDATA_PASSWORD=...
GEMS_API_KEY=...             # GEMS (nesc.nier.go.kr)

SATELLITE_BASE_DIR=/path/to/archive   # where all downloads and outputs go
```

> [!IMPORTANT]
> `SATELLITE_BASE_DIR` defaults to `./data` inside the repo. The archives reach terabytes,
> so point it at an external drive before the first real download.

> [!TIP]
> Downloading more than a few tens of GB of Sentinel-5P? Add the S3 keys — the default endpoint
> caps you at 4 connections and throttles (HTTP 429); S3 is several times faster.

Account sign-up, the S3 speed comparison and per-product resolutions: [docs/DATA_SOURCES.md](docs/DATA_SOURCES.md).
Where every file lands, and the other path variables: [docs/STORAGE.md](docs/STORAGE.md).

## Usage

Each hub has a one-call `run_pipeline()` (fetch → download → process); `fetch_data` /
`download_data` / `process_data` remain available step by step.

```python
from datetime import datetime
from src.api import SENTINEL5PHub, MODISHub, ERA5Hub, GEMSHub

SENTINEL5PHub(max_workers=3).run_pipeline(
    file_class='OFFL', file_type='NO2___',
    start_date=datetime(2025, 3, 1), end_date=datetime(2025, 3, 13),
    boundary=(120, 122, 22, 25))                     # (min_lon, max_lon, min_lat, max_lat)

MODISHub().run_pipeline(file_type='MCD19A2',
                        start_date=datetime(2025, 3, 1), end_date=datetime(2025, 3, 12))

GEMSHub().run_pipeline(product_type='NO2', start_date='2023-05-15', end_date='2023-05-15',
                       extract_bbox=(119, 123, 21, 26))

ERA5Hub(timezone='Asia/Taipei').run_pipeline(
    start_date=datetime(2025, 3, 1), end_date=datetime(2025, 3, 19),
    boundary=(119, 123, 21, 26), variables=['boundary_layer_height'],
    stations=[{"name": "TP", "lat": 25.033, "lon": 121.565}])
```

> [!NOTE]
> ERA5 writes each station's time series to CSV and does not render maps — pass `stations=`.

> [!TIP]
> GEMS `extract_bbox` crops on the server (~270 MB → 2–3 MB per granule), and the pipeline streams
> download → grid → delete raw, so a long backfill needs only a few MB of disk.

### Unified L3 (0.02° year files)

Footprint-supersampled binning puts every source on the same 251 × 201 grid over Taiwan:

```bash
python -m scripts.l3_regrid_year --source s5p   --product NO2___        --year 2024
python -m scripts.l3_regrid_year --source s5p   --product NO2___        --year 2023 --freq granule  # one step per orbit
python -m scripts.l3_regrid_year --source gems  --product GEMS_NO2_TROP --year 2024  # one file per time slot
python -m scripts.l3_regrid_year --source modis --product MCD19A2       --year 2025  # 550 nm + AOD_QA
```

> [!TIP]
> Add `--dry-run` to see how many raw files were found and where the output will go before a long run.

> [!WARNING]
> GEMS `ColumnAmountNO2` (`GEMS_NO2`) is the **total** column — mostly stratospheric over Taiwan.
> Use `GEMS_NO2_TROP` when comparing with TROPOMI or estimating surface sources.

Method, QC defaults, validation against HARP and output format: [src/processing/l3/README.md](src/processing/l3/README.md).

## Documentation

| Topic | Guide |
|---|---|
| Products, accounts, S3 downloads | [docs/DATA_SOURCES.md](docs/DATA_SOURCES.md) |
| Paths and directory layout | [docs/STORAGE.md](docs/STORAGE.md) |
| Unified L3 regrid pipeline | [src/processing/l3/README.md](src/processing/l3/README.md) |
| Coverage statistics | [src/coverage/README.md](src/coverage/README.md) |
| Merging per-granule files into series | [src/merge/README.md](src/merge/README.md) |
| GEMS API | [docs/GEMS_API_README.md](docs/GEMS_API_README.md) |
| MODIS AOD variables / HDF merge | [docs/MODIS_AOD_Variables_README.md](docs/MODIS_AOD_Variables_README.md), [docs/MODIS_HDF_Merge_README.md](docs/MODIS_HDF_Merge_README.md) |
| Himawari API *(mock, not wired to a real service)* | [docs/Himawari_API_README.md](docs/Himawari_API_README.md) |

## Contact

Bug reports and feature requests: [GitHub Issues](https://github.com/Alex870521/Satellite_S5P/issues).
