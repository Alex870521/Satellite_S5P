# Storage Layout

All downloads and outputs live under a single **base directory**, configured once via the
`SATELLITE_BASE_DIR` environment variable in `.env` (falls back to `./data` if unset).
Every satellite hub then builds the same `{base}/{Satellite}/{raw|processed|figure}/...` tree.

Anatomy of a downloaded file path (GEMS NO₂ example):

```
$SATELLITE_BASE_DIR / GEMS / raw /  NO2  / 2023/05 / GK2_GEMS_L2_..._NO2_..._.nc
└───── ① base ─────┘  └─②─┘ └─③─┘ └─④─┘ └──⑤──┘  └──────────── ⑥ ───────────┘
```

| # | Segment | Value (example) | Where it is set |
|---|---------|-----------------|-----------------|
| ① | **base dir** | `/Volumes/<drive>` | `.env` → `SATELLITE_BASE_DIR` (read by `src/config/settings.py` → `BASE_DIR`) |
| ② | **satellite** | `GEMS` | each hub's `name` attribute → `core.py` `main_dir = base_dir / name` |
| ③ | **stage** | `raw` (also `processed`, `figure`, `logs`) | `core.py` `_setup_common_dirs()` |
| ④ | **product** | `NO2` | hub download step (e.g. `gems_api.py`) |
| ⑤ | **year/month** | `2023/05` | derived from each file's timestamp |
| ⑥ | **filename** | original granule name | from the data provider |

Resulting tree:

```
$SATELLITE_BASE_DIR/
├── Sentinel-5P/ { raw, processed, figure }/L2/<product>/<YYYY>/<MM>/   (note the extra L2/ level; logs/ is flat)
├── MODIS/       { raw, figure, logs }/<product>/<YYYY>/<MM>/   processed/<product>/ is flat (no year/month dirs)
├── ERA5/        { raw, processed, figure, logs }/...
└── GEMS/        { raw, processed, figure, logs }/<product>/<YYYY>/<MM>/
    ├── raw/       NO2/2023/05/GK2_GEMS_L2_20230515_0345_NO2_..._.nc   ← downloaded swath
    ├── processed/ NO2/2023/05/GK2_GEMS_L2_20230515_0345_NO2_..._.nc   ← gridded NetCDF
    └── figure/    NO2/2023/05/GK2_GEMS_L2_20230515_0345_NO2_..._.png   ← map + monthly .gif
```

> [!TIP]
> **To relocate all data**, change only `SATELLITE_BASE_DIR` in `.env` — no code changes needed.

> [!NOTE]
> **Data on more than one drive?** Set `SATELLITE_DATA_ROOTS` (os.pathsep-separated, e.g.
> `/Volumes/Archive:/Volumes/Work`). Tools that look for the same product across drives —
> `scripts/l3_regrid_year.py` and the data-backed tests — search every root; unset means just
> `SATELLITE_BASE_DIR`. No path in the code names a specific drive, so swapping drives is an `.env` edit only.

## Local work files

Year files that analyses read directly are kept apart from the raw archive (set both in `.env`):

```
$LOCAL_WORK_DIR/                       (default ./data/work)
├── l3/          S5P_no2_l3_02deg_2024.nc                  ← unified L3, scripts/l3_regrid_year.py
│                S5P_no2_l3_02deg_2023_granule.nc          ← --freq granule (one step per orbit)
│                GEMS_no2_trop_l3_02deg_2024_0445UTC.nc     ← GEMS, one file per time slot
│                MODIS_mcd19a2_aod_l3_02deg_2025.nc
├── legacy_rbf/  S5P_NO2_20240101_20241231.nc              ← old 0.01° RBF merges
└── static/      taiwan_dem_gmrt.tif

$MOE_STATION_DIR/                      (default ./data/stations)
└── <YYYY>/      <site>_aqx_p_488_<YYYY>-01-01_<YYYY>-12-31.csv   ← scripts/download_epa_stations.py
```

| Variable | Default | Holds |
|---|---|---|
| `SATELLITE_BASE_DIR` | `./data` | Raw downloads, per-granule outputs, figures, logs — terabytes, normally an external drive |
| `SATELLITE_DATA_ROOTS` | `SATELLITE_BASE_DIR` | Extra drives searched for the same product |
| `LOCAL_WORK_DIR` | `./data/work` | Gridded year files (`l3/`, `legacy_rbf/`, `static/`) |
| `MOE_STATION_DIR` | `./data/stations` | MOENV hourly station CSVs |
