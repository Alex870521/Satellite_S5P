"""Regional data-coverage analysis across satellite hubs.

Reads each hub's *processed* nc files, computes — per time bucket — the fraction
of a region's grid cells carrying valid data, and returns a tidy table.

Quickstart:
    from src.coverage import compute_coverage
    df = compute_coverage("sentinel5p", "NO2___", "central",
                          "2023-01-01", "2023-12-31", granularity="monthly")

CLI:
    python -m src.coverage --hub sentinel5p --product NO2___ --region central \
        --start 2023-01-01 --end 2023-12-31 --granularity monthly

See SCHEMA.md for the raw-vs-processed file schema of every hub.
"""
from .engine import compute_coverage
from .base import Slice, CoverageRow
from .registry import HUB_SPECS, get_spec
from .reader import get_reader
from .region import AIR_QUALITY_ZONES, region_mask
from .plot import (plot_coverage_distribution, select_representative_days,
                   plot_representative_days)
from .qa_sweep import sweep_qa, plot_qa_sweep
from .plot import load_raw_l2, plot_raw_l2_pixels, plot_raw_l2_coverage
from .maps import per_cell_stats, plot_cell_stats

__all__ = [
    "compute_coverage", "Slice", "CoverageRow",
    "HUB_SPECS", "get_spec", "get_reader",
    "AIR_QUALITY_ZONES", "region_mask",
    "plot_coverage_distribution", "select_representative_days",
    "plot_representative_days", "sweep_qa", "plot_qa_sweep",
    "load_raw_l2", "plot_raw_l2_pixels", "plot_raw_l2_coverage",
    "per_cell_stats", "plot_cell_stats",
]
