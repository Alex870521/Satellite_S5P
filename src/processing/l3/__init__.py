"""統一 L3 regrid pipeline。

三個 source(S5P/GEMS/MODIS)共用同一條純 Python 路徑:
    Adapter(讀檔→GranuleL2) → SupersampleBinRegridder(footprint 超取樣 binning)
    → L3Writer / L3Accumulator

SupersampleBinRegridder 取代 HARP runtime(多軌驗證 r≈0.999),HARP 僅作離線 oracle。
"""
from src.processing.l3.granule import GranuleL2, GridSpec, GriddedField
from src.processing.l3.regridder import (
    SupersampleBinRegridder,
    RbfRegridder,
    corners_from_centers,
)
from src.processing.l3.writer import L3Writer
from src.processing.l3.pipeline import L3Pipeline, L3Accumulator
from src.processing.l3.adapters import S5PAdapter

__all__ = [
    "GranuleL2", "GridSpec", "GriddedField",
    "SupersampleBinRegridder", "RbfRegridder", "corners_from_centers",
    "L3Writer", "L3Pipeline", "L3Accumulator", "S5PAdapter",
]
