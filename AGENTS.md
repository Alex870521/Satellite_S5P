# AGENTS.md

Guidance for AI coding agents working in this repository. Human-facing docs: [README.md](README.md),
[CONTRIBUTING.md](CONTRIBUTING.md), [docs/](docs/).

## What this is

A toolkit that downloads, grids and plots atmospheric satellite data (Sentinel-5P, MODIS, GEMS, ERA5),
plus a unified L3 pipeline that regrids every source onto one 0.02° grid over Taiwan (251 × 201).

## Layout

| Path | What lives there |
|---|---|
| `src/api/` | One hub per source (`SENTINEL5PHub`, `MODISHub`, `GEMSHub`, `ERA5Hub`): fetch → download → process |
| `src/processing/l3/` | Unified L3 pipeline (adapters, supersampling regridder, aggregation, writer) — see its README |
| `src/processing/*_processor.py` | Older per-granule processors (RBF). Still supported; do not delete or restructure them |
| `src/config/` | `catalog.py` = product metadata (single source of truth); `settings.py` = paths from `.env` |
| `src/coverage/`, `src/merge/`, `src/visualization/` | Coverage stats, series merging, plotting (`plot_nc.basic_map` has many downstream callers) |
| `scripts/` | CLIs, e.g. `l3_regrid_year.py`, `download_epa_stations.py` |
| `tests/` | pytest; `requires_data` marks tests that need the external drives |

`.codegraph/` (local, gitignored) indexes this repo: prefer `codegraph explore "<symbols or question>"`
over grep when locating code.

## Commands

```bash
pip install -e ".[dev]"          # + ".[ingest]" for raw MODIS .hdf (pyhdf; Python 3.12/3.13 only)
pytest -m "not requires_data"     # what CI runs
mypy                              # what CI runs; scope is set in pyproject.toml
python -m scripts.l3_regrid_year --source s5p --product NO2___ --year 2024 --dry-run
```

Both CI checks must pass. Tests that need data skip themselves when the drives are absent.

## Rules

- **Research defaults are deliberate** — S5P `qa` 0.5, supersampling K=4, 0.02° grid, product colour
  scales, GEMS `mask_negative`, no negative-value filtering for TROPOMI. Expose a change as an option;
  never edit the default to "fix" it.
- **Paths come from the environment** (`SATELLITE_BASE_DIR`, `SATELLITE_DATA_ROOTS`, `LOCAL_WORK_DIR`,
  `MOE_STATION_DIR`; see [docs/STORAGE.md](docs/STORAGE.md)). Never hard-code a drive or home directory.
- **Product metadata belongs in `src/config/catalog.py`**, not inside a processor or adapter.
- Changes to gridding or QC need a test, and a note in `src/processing/l3/README.md` when output values change.
- Commit messages: imperative subject, a body that says why.

## Gotchas

- S5P L2 `time` is only the 00:00 reference of the day; per-scanline times are in `delta_time`
  (the L3 pipeline derives the overpass time from the scanlines inside the grid).
- GEMS `ColumnAmountNO2` (`GEMS_NO2`) is the **total** column, mostly stratospheric; use `GEMS_NO2_TROP`
  for tropospheric work. GEMS L3 dates are Taiwan local dates (UTC+8), one file per time slot.
- MCD19A2: the L3 adapter reads 550 nm with `AOD_QA` best; `MODISProcessor` itself still defaults to
  470 nm without QA. MCD19A2 tiles are daily composites — `--freq granule` is invalid for them.
- `torch` and `lightgbm` in the same process segfault or deadlock (OpenMP). Run LightGBM in a separate
  `python -m` subprocess whose module never imports torch.
- Writers and CLIs never overwrite the daily year file by accident: non-daily outputs get
  `_granule` / `_M` / `_Y` suffixes, and the three MODIS products have distinct file names.
