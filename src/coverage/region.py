"""Region-of-interest masks on a slice's grid.

Two region kinds:
  * **bbox**     — a (lon_min, lon_max, lat_min, lat_max) box. Named boxes come
                   from ``settings.REGIONS`` ('taiwan', 'east_asia').
  * **polygon**  — a set of counties from the MOI county shapefile, unioned into
                   one polygon (the air-quality zones 'central', 'zhumiao', ...).

Masks are cached per (grid signature, region) so a year of same-grid slices only
pays the point-in-polygon cost once.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np

from src.config.settings import REGIONS

_REPO = Path(__file__).resolve().parents[2]
COUNTIES_SHP = _REPO / "data" / "shapefiles" / "taiwan" / "COUNTY_MOI_1090820.shp"

# Air-quality zones (county groupings), mirroring wip_coverage definitions.
AIR_QUALITY_ZONES = {
    "north":   ["臺北市", "新北市", "基隆市", "桃園市", "宜蘭縣"],
    "zhumiao": ["新竹縣", "新竹市", "苗栗縣"],
    "central": ["臺中市", "彰化縣", "南投縣", "雲林縣"],
    "yunchianan": ["嘉義縣", "嘉義市", "臺南市"],
    "kaoping": ["高雄市", "屏東縣"],
}

_mask_cache: dict = {}
_poly_cache: dict = {}


def _zone_polygon(zone: str):
    if zone in _poly_cache:
        return _poly_cache[zone]
    import geopandas as gpd
    from shapely.ops import unary_union
    gdf = gpd.read_file(COUNTIES_SHP)
    counties = AIR_QUALITY_ZONES[zone]
    sub = gdf[gdf["COUNTYNAME"].isin(counties)]
    if sub.empty:
        raise ValueError(f"No counties matched for zone '{zone}': {counties}")
    poly = unary_union(sub.geometry.values)
    _poly_cache[zone] = poly
    return poly


def _grid_2d(lats: np.ndarray, lons: np.ndarray):
    """Return 2-D (lat2d, lon2d) cell-centre grids regardless of input rank."""
    if lats.ndim == 1 and lons.ndim == 1:
        lon2d, lat2d = np.meshgrid(lons, lats)
    else:
        lat2d, lon2d = lats, lons
    return lat2d, lon2d


def region_mask(lats: np.ndarray, lons: np.ndarray, region: str,
                signature: tuple) -> np.ndarray:
    """Boolean mask (same shape as the value field) of cells inside *region*."""
    cache_key = (signature, region)
    if cache_key in _mask_cache:
        return _mask_cache[cache_key]

    lat2d, lon2d = _grid_2d(lats, lons)

    if region in REGIONS:  # named bounding box
        lo_lon, hi_lon, lo_lat, hi_lat = REGIONS[region]
        mask = ((lon2d >= lo_lon) & (lon2d <= hi_lon)
                & (lat2d >= lo_lat) & (lat2d <= hi_lat))
    elif region in AIR_QUALITY_ZONES:  # county polygon
        import shapely
        poly = _zone_polygon(region)
        # shapely 2.x vectorised point-in-polygon; correct for MultiPolygon.
        mask = shapely.contains_xy(poly, lon2d, lat2d)
    else:
        raise ValueError(
            f"Unknown region '{region}'. Boxes: {sorted(REGIONS)}; "
            f"zones: {sorted(AIR_QUALITY_ZONES)}")

    _mask_cache[cache_key] = mask
    return mask


def cell_lats_2d(lats: np.ndarray, lons: np.ndarray) -> np.ndarray:
    """Latitude of every cell (for area weighting)."""
    lat2d, _ = _grid_2d(lats, lons)
    return lat2d
