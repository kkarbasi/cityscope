<p align="center">
  <img src="src/urban_research/dashboard/logo.svg" alt="Urban Research" width="420">
</p>

<p align="center">
  <strong>Find your next real estate market in minutes, not months.</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/python-3.13+-3776ab?logo=python&logoColor=white" alt="Python 3.13+">
  <img src="https://img.shields.io/badge/data-Census%20%7C%20BLS-1e3a5f" alt="Data Sources">
  <img src="https://img.shields.io/badge/storage-SQLite-003b57?logo=sqlite&logoColor=white" alt="SQLite">
  <img src="https://img.shields.io/badge/dashboard-Streamlit-ff4b4b?logo=streamlit&logoColor=white" alt="Streamlit">
</p>

---

Urban Research is an open-source data pipeline and interactive dashboard that pulls public government data about every major US metro and city — population growth, job growth, wages, unemployment — and lets you explore it all locally. No subscriptions, no paywalls, no stale spreadsheets.

## What You Get

**370+ metros and cities** with 200k+ population, each tracked across **9 metrics** over **5 years** (2020–2024):

| Category | Metrics | Source |
|---|---|---|
| Population | Total population, YoY change, growth % | US Census Bureau (PEP + ACS) |
| Employment | Total jobs, YoY change, job growth % | Bureau of Labor Statistics (QCEW) |
| Wages | Average annual pay, average weekly wage | Bureau of Labor Statistics (QCEW) |
| Unemployment | Annual average unemployment rate | Bureau of Labor Statistics (LAUS) |

All data is **free, public domain**, pulled directly from federal APIs — no scraping, no third-party dependencies.

## Quick Start

Requires Python 3.13+ and [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/kkarbasi/urban-research.git
cd urban-research
uv sync

# Pull all data (~30 seconds)
uv run urban-research fetch census_population
uv run urban-research fetch bls_employment --skip-laus

# Launch the dashboard
uv run urban-research dashboard
```

Open **http://localhost:8501** and start exploring.

## Dashboard

Four tabs designed for real estate research:

- **Rankings** — Rank metros by population growth, job growth, unemployment, or average pay. Horizontal bar chart + full sortable table.
- **Trends** — Compare up to 15 metros side-by-side with line charts. Population, employment, wages, cumulative growth.
- **City Profile** — Deep dive into a single metro: every metric charted over time in one view.
- **Data Explorer** — Full data table with filters. Download any slice as CSV.

## CLI

```bash
# Top 20 fastest-growing metros
uv run urban-research query -m population_change_pct -g metro -y 2024

# Top 10 by job growth
uv run urban-research query -m employment_change_pct -g metro -y 2024 -n 10

# Highest-paying metros
uv run urban-research query -m avg_annual_pay -y 2024 -n 15

# What data do I have?
uv run urban-research status
```

## All Commands

| Command | What it does |
|---|---|
| `fetch <source>` | Pull data from a source (`census_population`, `bls_employment`) |
| `fetch --all` | Pull from all sources |
| `query` | Query stored data with filters (`-m`, `-g`, `-y`, `--min-pop`, `-n`) |
| `sources` | List registered data sources |
| `status` | Show fetched data summary |
| `dashboard` | Launch Streamlit dashboard (default port 8501) |
| `init-config` | Generate default `config/settings.yaml` |

Global flags: `-v` (verbose logging), `-c PATH` (custom config file).

## Fetch Options

| Flag | Description |
|---|---|
| `--vintage YEAR` | Override Census data vintage year |
| `--min-pop N` | Override population filter (default: 200,000) |
| `--skip-laus` | Skip unemployment rate (avoids BLS API daily limit) |

## Configuration

Optional. Works out of the box with no config. For higher API limits:

```yaml
# config/settings.yaml
census:
  api_key: null    # Free: https://api.census.gov/data/key_signup.html
bls:
  api_key: null    # Free: https://data.bls.gov/registrationEngine/
storage:
  db_path: data/urban_research.db
pipeline:
  min_population: 200000
```

## Architecture

```
Census API ──┐                  ┌── CLI (query, status)
             ├── Pipeline ── SQLite DB ──┤
BLS QCEW  ──┘    (fetch)       └── Streamlit Dashboard
```

The framework is designed to be extended. Each data source is a self-contained Python class:

```python
from urban_research.core.registry import SourceRegistry
from urban_research.core.source import DataSource

@SourceRegistry.register
class MyNewSource(DataSource):
    source_id = "my_source"
    name = "My Data Source"
    description = "What it provides"

    def fetch(self, **kwargs) -> FetchResult:
        # Fetch → transform → return FetchResult
        ...
```

Add the import to `src/urban_research/sources/__init__.py` and it auto-registers everywhere — CLI, pipeline, dashboard.

## Direct Database Access

The SQLite database at `data/urban_research.db` is yours to query however you want:

```python
import sqlite3, pandas as pd

conn = sqlite3.connect("data/urban_research.db")
df = pd.read_sql("""
    SELECT g.name, d.year, d.value
    FROM data_points d
    JOIN geographies g ON d.geo_id = g.geo_id
    WHERE d.metric = 'employment_change_pct'
      AND g.geo_type = 'metro'
      AND d.year = 2024
    ORDER BY d.value DESC
    LIMIT 20
""", conn)
```

## Roadmap

- [ ] Rent data (HUD Fair Market Rents, Zillow ZORI)
- [ ] Home price index (FHFA HPI)
- [ ] Crime stats (FBI Crime Data Explorer)
- [ ] School quality (NCES)
- [ ] Walkability scores (EPA Smart Location Database)
- [ ] Migration flows (IRS SOI county-to-county)
- [ ] Neighborhood-level data (Census tract)
- [ ] Composite "investability" scoring

See [`data_sources.md`](data_sources.md) for the full research on 50+ public data sources that can feed this project.

## Contributing

Pull requests welcome. The easiest way to contribute is adding a new data source — the plugin architecture makes it straightforward. See the **Architecture** section above.

## License

MIT

---

Built with [Claude Code](https://claude.ai/claude-code) (Claude Opus 4.6).
