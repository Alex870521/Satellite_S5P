# Contributing

Bug reports and pull requests are welcome via [GitHub Issues](https://github.com/Alex870521/Satellite_S5P/issues).

## Development setup

```bash
git clone https://github.com/Alex870521/Satellite_S5P.git
cd Satellite_S5P
pip install -e ".[dev]"        # add ".[ingest]" to read raw MODIS .hdf (Python 3.12/3.13)
```

Credentials and data paths go in `.env` — see the [README](README.md#configuration) and
[docs/STORAGE.md](docs/STORAGE.md).

## Before opening a PR

CI runs these two commands and fails on either:

```bash
pytest -m "not requires_data"   # logic tests, no satellite files needed
mypy                            # type check, scope set in pyproject.toml
```

Tests marked `requires_data` read real files from `SATELLITE_DATA_ROOTS` and skip when they are
absent; run `pytest -m requires_data` locally if you change anything that touches readers or the
L3 pipeline.

## Guidelines

- Keep the research defaults (S5P `qa` 0.5, supersampling K=4, 0.02° grid, product colour scales):
  change them through options, not by editing the default.
- New data paths come from `.env` / environment variables; never hard-code a drive or user directory.
- A change to gridding or QC needs a test, and a note in [src/processing/l3/README.md](src/processing/l3/README.md)
  if it changes output values.
