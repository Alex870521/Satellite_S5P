"""Per-hub description of *processed* NetCDF layout.

This is the single source of truth the generic reader uses to open any hub's
processed files offline (no authentication, no hub instantiation). It mirrors
what each hub's ``process_data`` writes — see ``src/coverage/SCHEMA.md`` for the
full raw-vs-processed schema reference.

Resolution / variable names verified against the live archive 2026-06:
  Sentinel-5P  processed/<PRODUCT>/<YYYY>/<MM>/*.nc   dims (time=1, latitude, longitude)
  MODIS        processed/<PRODUCT>/*.nc               dims (time=N, lat, lon)  (pre-gridded 0.1°)
  GEMS         processed/<PRODUCT>/<YYYY>/<MM>/*.nc   dims (time=1, latitude, longitude)
  Sentinel-3   processed/<PRODUCT>/<YYYY>/<MM>/*.nc   (same base as S5P; no data archived yet)
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class HubSpec:
    """How to find and read one hub's processed files.

    Attributes:
        key:        canonical hub id used on the CLI (``--hub``).
        dir_name:   sub-folder of BASE_DIR holding this hub's data.
        layout:     ``year_month`` (PRODUCT/YYYY/MM/*.nc) or ``flat`` (PRODUCT/*.nc).
        lat_names:  candidate latitude coord names, first match wins.
        lon_names:  candidate longitude coord names.
        time_names: candidate time coord names.
        variables:  product -> data-variable name. ``None`` value => use the
                    file's first/only data variable (robust default).
        qa_in_processed: processed files are already QC-filtered (NaN = masked),
                    so the reader does not re-apply a QA threshold.
        kind:       ``gridded_nc`` (generic reader), ``csv`` (ERA5), ``mock``
                    (Himawari placeholder).
    """

    key: str
    dir_name: str
    layout: str = "year_month"
    lat_names: tuple = ("latitude", "lat")
    lon_names: tuple = ("longitude", "lon")
    time_names: tuple = ("time",)
    variables: dict = field(default_factory=dict)
    qa_in_processed: bool = True
    kind: str = "gridded_nc"
    # 處理階層子層(若有):Sentinel-5P 的 processed 為 processed/<level>/<PRODUCT>/...
    # (2026-06 起 raw/processed/figure/geotiff 都在 L2/L3 之下)。None = 無此層(GEMS/MODIS)。
    level: str | None = None


# Sentinel-5P / Sentinel-3 share SentinelHubBase => identical processed schema.
_S5P_VARS = {
    "NO2___": "nitrogendioxide_tropospheric_column",
    "O3____": "ozone_total_vertical_column",
    "SO2___": "sulfurdioxide_total_vertical_column",
    "HCHO__": "formaldehyde_tropospheric_vertical_column",
    "CH4___": "methane_mixing_ratio",
    "CO____": "carbonmonoxide_total_column",
    "AER_AI": "aerosol_index_354_388",
}

_GEMS_VARS = {
    "NO2": "ColumnAmountNO2",
    "O3T": "ColumnAmountO3",
    "HCHO": "ColumnAmountHCHO",
    "SO2": "ColumnAmountSO2",
    # AERAOD is multi-wavelength -> variable resolved at read time; leave None.
    "AERAOD": None,
}

HUB_SPECS = {
    "sentinel5p": HubSpec(
        key="sentinel5p", dir_name="Sentinel-5P", layout="year_month",
        variables=_S5P_VARS, level="L2",
    ),
    "sentinel3": HubSpec(
        key="sentinel3", dir_name="Sentinel-3", layout="year_month",
        variables={}, level="L2",  # resolve to first data var until products are archived
    ),
    "gems": HubSpec(
        key="gems", dir_name="GEMS", layout="year_month",
        variables=_GEMS_VARS,
    ),
    "modis": HubSpec(
        key="modis", dir_name="MODIS", layout="flat",
        lat_names=("lat", "latitude"), lon_names=("lon", "longitude"),
        variables={"MYD04_L2": "aod", "MOD04_L2": "aod", "MCD19A2": "aod",
                   # faithful drop-in-the-box binned cubes (modis_daily_grid.py)
                   "MYD04_L2_binned": "aod", "MOD04_L2_binned": "aod"},
    ),
    "era5": HubSpec(
        key="era5", dir_name="ERA5", layout="flat",
        kind="csv",  # ERA5 processed output is per-station CSV, not gridded nc
    ),
    "himawari": HubSpec(
        key="himawari", dir_name="Himawari", layout="year_month",
        kind="mock",  # hub is still a stub; no processor yet
    ),
}


def get_spec(hub: str) -> HubSpec:
    key = hub.lower().replace("-", "").replace("_", "")
    aliases = {"s5p": "sentinel5p", "sentinel5p": "sentinel5p",
               "s3": "sentinel3", "sentinel3": "sentinel3"}
    key = aliases.get(key, key)
    if key not in HUB_SPECS:
        raise ValueError(f"Unknown hub '{hub}'. Known: {sorted(HUB_SPECS)}")
    return HUB_SPECS[key]
