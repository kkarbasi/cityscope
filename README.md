<p align="center">
  <img src="https://raw.githubusercontent.com/kkarbasi/cityscope/main/assets/logo.svg" alt="Cityscope" width="420">
</p>

<p align="center">
  <strong>Find your next real estate market in minutes, not months.</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/python-3.13+-3776ab?logo=python&logoColor=white" alt="Python 3.13+">
  <img src="https://img.shields.io/badge/data-Census%20%7C%20BLS-4f46e5" alt="Data Sources">
  <img src="https://img.shields.io/badge/storage-SQLite-003b57?logo=sqlite&logoColor=white" alt="SQLite">
  <img src="https://img.shields.io/pypi/v/cityscope?color=6366f1&cache=v1" alt="PyPI">
  <img src="https://img.shields.io/github/license/kkarbasi/cityscope" alt="License">
</p>

---

A pip-installable Python package that pulls public government data about every major US metro and city — population growth, job growth, wages, unemployment — and stores it locally in SQLite for analysis. Use it as a **library**, from the **CLI**, or build your own UI on top.

## Install

```bash
pip install cityscope
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv add cityscope
```

## Python API

```python
from cityscope import api

# Fetch data from public sources
api.fetch("census_population")
api.fetch("bls_employment", skip_laus=True)

# Query as a DataFrame
df = api.to_dataframe(metric="population_change_pct", geo_type="metro", year=2024)
print(df.sort_values("value", ascending=False).head(10))

# Or as a list of dicts
rows = api.query(metric="employment_change_pct", geo_type="metro", year=2024, limit=10)
for row in rows:
    print(f"{row['name']}: {row['value']:+.1f}% job growth")

# Configure (optional — works with defaults)
api.configure(db_path="my_data.db", min_population=100_000)

# Check what you have
api.status()
api.list_sources()
api.get_geographies(geo_type="metro", min_population=500_000)
```

## CLI

```bash
# Fetch data
cityscope fetch census_population
cityscope fetch bls_employment --skip-laus
cityscope fetch --all

# Query
cityscope query -m population_change_pct -g metro -y 2024
cityscope query -m employment_change_pct -g metro -y 2024 -n 10
cityscope query -m avg_annual_pay -y 2024 -n 15

# Info
cityscope sources
cityscope status
```

### Commands

| Command | Description |
|---|---|
| `fetch <source>` | Pull data from a source (`census_population`, `bls_employment`) |
| `fetch --all` | Pull from all sources |
| `query` | Query stored data (`-m`, `-g`, `-y`, `--min-pop`, `-n`) |
| `sources` | List available data sources |
| `status` | Show fetched data summary |
| `init-config` | Generate default `config/settings.yaml` |

Global flags: `-v` (verbose logging), `-c PATH` (custom config file).

### Fetch Flags

| Flag | Description |
|---|---|
| `--vintage YEAR` | Override Census vintage year |
| `--min-pop N` | Override population filter (default: 200,000) |
| `--skip-laus` | Skip unemployment rate (avoids BLS API daily limit) |

## What Data You Get

**370+ metros and cities** (200k+ population), each tracked across **9 metrics** over **5 years** (2020–2024):

| Metric | Source | Description |
|---|---|---|
| `population` | Census PEP/ACS | Total population |
| `population_change_pct` | Census PEP/ACS | Year-over-year population growth % |
| `employment` | BLS QCEW | Total nonfarm jobs |
| `employment_change_pct` | BLS QCEW | Year-over-year job growth % |
| `avg_annual_pay` | BLS QCEW | Average annual pay ($) |
| `avg_weekly_wage` | BLS QCEW | Average weekly wage ($) |
| `unemployment_rate` | BLS LAUS | Annual avg unemployment rate (%) |

All data is **free, public domain**, pulled directly from federal APIs.

## Configuration

Optional — everything works with defaults. For higher API limits:

```yaml
# config/settings.yaml
census:
  api_key: null    # Free: https://api.census.gov/data/key_signup.html
bls:
  api_key: null    # Free: https://data.bls.gov/registrationEngine/
storage:
  db_path: data/cityscope.db
pipeline:
  min_population: 200000
```

Or configure programmatically:

```python
api.configure(census_api_key="your_key", bls_api_key="your_key")
```

## Architecture

```
Census API ──┐                     ┌── Python API (cityscope.api)
             ├── Pipeline ── SQLite DB ──┤
BLS QCEW  ──┘    (fetch)          └── CLI (cityscope)
```

Data flows: **Source → Pipeline → SQLite → API/CLI/your code**.

## Adding Data Sources

Each source is a self-contained class with `@SourceRegistry.register`:

```python
from cityscope.core.registry import SourceRegistry
from cityscope.core.source import DataSource
from cityscope.core.models import FetchResult

@SourceRegistry.register
class MySource(DataSource):
    source_id = "my_source"
    name = "My Data Source"
    description = "What it provides"

    def fetch(self, **kwargs) -> FetchResult:
        ...
```

Add the import to `src/cityscope/sources/__init__.py` — it auto-registers in CLI, API, and pipeline.

## Dashboard

For a visual dashboard, see [urban-research-ui](https://github.com/kkarbasi/urban-research-ui).

## Roadmap

- [ ] Rent data (HUD Fair Market Rents, Zillow ZORI)
- [ ] Home price index (FHFA HPI)
- [ ] Crime stats (FBI Crime Data Explorer)
- [ ] School quality (NCES)
- [ ] Walkability (EPA Smart Location Database)
- [ ] Migration flows (IRS SOI county-to-county)
- [ ] Neighborhood-level data (Census tract)
- [ ] Composite scoring engine

See [`data_sources.md`](data_sources.md) for research on 50+ public data sources.

## Contributing

Pull requests welcome. The easiest way to contribute is adding a new data source — the plugin architecture makes it straightforward.

## License

MIT

---

Built with [Claude Code](https://claude.ai/claude-code) (Claude Opus 4.6).
