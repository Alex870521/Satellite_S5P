# Data Sources

Products, resolutions and account setup for each hub. Credentials go in `.env` (see the README).

## Sentinel-5P (TROPOMI)

ESA / Copernicus. Daily global coverage; processing classes `NRTI` / `OFFL` / `RPRO`.
Auth: `COPERNICUS_USERNAME` / `COPERNICUS_PASSWORD` ([Copernicus Data Space](https://dataspace.copernicus.eu)).

| Product | `file_type` | Resolution (km) | Quantity |
|---|---|---|---|
| NO₂ | `NO2___` | 5.5 × 3.5 | Tropospheric column |
| O₃ | `O3____` | 5.5 × 3.5 | Total column |
| O₃ profile | `O3__PR` | 30 × 30 | Vertical profile |
| SO₂ | `SO2___` | 5.5 × 3.5 | Total column |
| HCHO | `HCHO__` | 5.5 × 3.5 | Tropospheric column |
| CO | `CO____` | 5.5 × 7 | Total column |
| CH₄ | `CH4___` | 5.5 × 7 | Column-averaged mixing ratio |
| Aerosol Index | `AER_AI` | 5.5 × 3.5 | UV aerosol index (`aerosol_index_340_380` default, `_354_388` selectable) |
| Cloud | `CLOUD_` | 5.5 × 3.5 | Download only — no processing config yet |

Nadir resolution is 5.5 × 3.5 km since 2019-08-06 (7 × 3.5 km before).

### Bulk downloads: use S3

The OData/zipper endpoint allows **4 concurrent connections**; more returns HTTP 429 and throughput
collapses (measured 85 MB/s for a minute, then 2 MB/s for an hour). The S3 endpoint has its own quota:

| Route | 1 connection | Practical |
|---|---|---|
| OData / zipper | 3.4 MB/s | 4 conns ≈ 15 MB/s, more ⇒ 429 |
| **S3 `eodata`** | **9.3 MB/s** | 8 conns ≈ 36 MB/s |

*(Measured 2026-09-22 from Taiwan.)* Both routes can run at the same time. Keys:
[eodata-s3keysmanager](https://eodata-s3keysmanager.dataspace.copernicus.eu) → `S3_ACCESS_KEY` / `S3_SECRET_KEY`;
endpoint `https://eodata.dataspace.copernicus.eu`, bucket `eodata`, object key = the product's OData
`S3Path` without the leading `/eodata/`.

## MODIS

NASA, Terra & Aqua. 1–2 day global coverage.
Auth: `EARTHDATA_USERNAME` / `EARTHDATA_PASSWORD` ([NASA Earthdata](https://urs.earthdata.nasa.gov/)).

| Product | Platform | Algorithm | Resolution | AOD read |
|---|---|---|---|---|
| `MOD04_L2` / `MYD04_L2` | Terra / Aqua | Dark Target + Deep Blue (L2) | 10 km | `AOD_550_Dark_Target_Deep_Blue_Combined` |
| `MOD04_3K` / `MYD04_3K` | Terra / Aqua | Dark Target (L2) | 3 km | — |
| `MCD19A2` | Terra + Aqua | MAIAC (L3 tiles) | 1 km | L3: `Optical_Depth_055` + `AOD_QA` best (`--aod-band/--aod-qa`) |

Variable reference: [MODIS_AOD_Variables_README.md](MODIS_AOD_Variables_README.md).

> [!TIP]
> **`Token does not exist`** from search/download: a stale bearer token on the `cmr.earthdata.nasa.gov`
> line of `~/.netrc` is being sent by `python-cmr` — remove that line or regenerate the token.

## ERA5

ECMWF reanalysis, hourly, 0.25° global. Auth: `CDSAPI_URL` / `CDSAPI_KEY`
([Climate Data Store](https://cds.climate.copernicus.eu/)).

| Type | Examples | Levels |
|---|---|---|
| Single level | boundary_layer_height, 2 m temperature, 10 m wind, mean sea-level pressure | surface |
| Pressure level | temperature, u/v wind, geopotential, relative humidity | 37 (1000–1 hPa) |

> [!NOTE]
> The ERA5 hub extracts per-station time series to CSV; it does not render maps.

## GEMS

NIER/NESC (Korea), GK-2B geostationary. Hourly, **daytime only**, East/South-East Asia (up to ~10 scans/day).
Auth: `GEMS_API_KEY` ([nesc.nier.go.kr](https://nesc.nier.go.kr), "single key" mode).

| Level | Products | Resolution | Since |
|---|---|---|---|
| L2 (swath) | NO₂, O₃ (O3T), SO₂, HCHO, CHOCHO, AOD/AEH, UVI, Cloud | 3.5 × 8 km (N–S × E–W) at Seoul; ≈ 2.8 × 7.1 km over Taiwan | ~2020-09 |
| L3 (gridded) | NO₂ daily / monthly (column & tropospheric) | ~5 km Korea, ~10 km elsewhere | ~2020-09 |
| L4 (surface) | PM₂.₅, PM₁₀, NO₂ | gridded | ~2021-12 |

> [!WARNING]
> `ColumnAmountNO2` is the **total** column; compare with TROPOMI using the tropospheric product
> (`GEMS_NO2_TROP`).

API details: [GEMS_API_README.md](GEMS_API_README.md).
