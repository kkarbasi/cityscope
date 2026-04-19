# Urban Research

A data pipeline and dashboard for real estate investment research. Pulls public data about US metro areas and cities — population growth, employment, wages, unemployment — and stores it locally for analysis.

## Setup

Requires Python 3.13+ and [uv](https://docs.astral.sh/uv/).

```bash
# Clone and install
cd urban_research
uv sync

# Generate default config (optional — works without it)
uv run urban-research init-config
```

## Quick Start

```bash
# 1. Fetch population data (metros + cities with 200k+ population)
uv run urban-research fetch census_population

# 2. Fetch employment and wage data
uv run urban-research fetch bls_employment --skip-laus

# 3. Launch the dashboard
uv run urban-research dashboard
# Open http://localhost:8501
```

## CLI Reference

All commands support `--help` for details. Global options go **before** the command name.

### Global Options

| Flag | Description |
|---|---|
| `-v`, `--verbose` | Enable debug logging |
| `-c`, `--config PATH` | Path to config YAML (default: `config/settings.yaml`) |

### `fetch` — Pull data from sources

```bash
uv run urban-research fetch <source_id> [OPTIONS]
uv run urban-research fetch --all [OPTIONS]
```

| Argument / Flag | Description |
|---|---|
| `source_id` | Which source to fetch (see `sources` command) |
| `--all` | Fetch from all registered sources |
| `--vintage YEAR` | Override data vintage year (Census only) |
| `--min-pop N` | Override minimum population filter (default: 200,000) |
| `--skip-laus` | Skip BLS LAUS unemployment rate fetch (avoids API rate limit) |

**Examples:**

```bash
# Fetch just population data
uv run urban-research fetch census_population

# Fetch employment + wages (skip unemployment rate to avoid BLS API limit)
uv run urban-research fetch bls_employment --skip-laus

# Fetch everything, include cities down to 100k
uv run urban-research fetch --all --min-pop 100000

# Fetch with verbose logging to debug API issues
uv run urban-research -v fetch census_population
```

### `sources` — List available data sources

```bash
uv run urban-research sources
```

Current sources:

| ID | What it fetches | Data from |
|---|---|---|
| `census_population` | Population, population change, growth % for metros and cities | Census PEP + ACS |
| `bls_employment` | Employment, job growth, avg pay, avg weekly wage, unemployment rate | BLS QCEW + LAUS |

### `query` — Query stored data from the terminal

```bash
uv run urban-research query [OPTIONS]
```

| Flag | Description |
|---|---|
| `-m`, `--metric NAME` | Filter by metric (e.g. `population_change_pct`, `employment`) |
| `-g`, `--geo-type TYPE` | Filter by geography type (`metro` or `city`) |
| `-y`, `--year YEAR` | Filter by year |
| `--min-pop N` | Minimum population |
| `-n`, `--top N` | Number of rows to show (default: 20) |

**Examples:**

```bash
# Top 20 metros by population growth in 2024
uv run urban-research query -m population_change_pct -g metro -y 2024

# Top 10 metros by job growth
uv run urban-research query -m employment_change_pct -g metro -y 2024 -n 10

# Cities with highest average pay
uv run urban-research query -m avg_annual_pay -g metro -y 2024 -n 15

# All data for metros with 500k+ population
uv run urban-research query -g metro --min-pop 500000
```

### `status` — Show what data has been fetched

```bash
uv run urban-research status
```

Shows each source, metric, record count, year range, and last fetch time.

### `dashboard` — Launch the web dashboard

```bash
uv run urban-research dashboard [OPTIONS]
```

| Flag | Description |
|---|---|
| `-p`, `--port N` | Port number (default: 8501) |

Opens a Streamlit dashboard at `http://localhost:8501` with four tabs:

- **Rankings** — Bar chart + table, rank metros by population growth, job growth, unemployment rate, or average pay
- **Trends** — Line charts comparing up to 15 metros across any metric over time
- **City Profile** — Deep dive into a single metro with all metrics charted
- **Data Explorer** — Full data table with metric/source filters and CSV download

### `init-config` — Generate a default config file

```bash
uv run urban-research init-config [--path PATH]
```

## Available Metrics

| Metric | Source | Description |
|---|---|---|
| `population` | Census PEP/ACS | Total population |
| `population_change` | Census PEP/ACS | Year-over-year population change |
| `population_change_pct` | Census PEP/ACS | Year-over-year population growth % |
| `employment` | BLS QCEW | Total nonfarm employment (jobs) |
| `employment_change` | BLS QCEW | Year-over-year job change |
| `employment_change_pct` | BLS QCEW | Year-over-year job growth % |
| `avg_annual_pay` | BLS QCEW | Average annual pay ($) |
| `avg_weekly_wage` | BLS QCEW | Average weekly wage ($) |
| `unemployment_rate` | BLS LAUS | Annual avg unemployment rate (%) |

## Configuration

Edit `config/settings.yaml`:

```yaml
census:
  # Free key at https://api.census.gov/data/key_signup.html
  # Works without one (500 req/day), key removes limits.
  api_key: null

bls:
  # Free key at https://data.bls.gov/registrationEngine/
  # Only needed for LAUS unemployment data (increases limit from 25 to 500 req/day).
  api_key: null

storage:
  db_path: data/urban_research.db

pipeline:
  min_population: 200000
  default_vintage: null  # null = auto-detect latest
```

## Data Storage

All data is stored locally in a SQLite database at `data/urban_research.db`. Two tables:

- **`geographies`** — one row per metro/city (ID, name, type, population)
- **`data_points`** — one row per metric per year per geography

Re-running `fetch` upserts (updates existing records, inserts new ones) — safe to re-run anytime.

You can also query the database directly:

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

## Adding New Data Sources

Create a new file in `src/urban_research/sources/`, subclass `DataSource`, and register it:

```python
from ..core.registry import SourceRegistry
from ..core.source import DataSource

@SourceRegistry.register
class MyNewSource(DataSource):
    source_id = "my_source"
    name = "My Data Source"
    description = "What it provides"

    def fetch(self, **kwargs) -> FetchResult:
        # Fetch data, return FetchResult with geographies + data points
        ...
```

Then add the import to `src/urban_research/sources/__init__.py`:

```python
from . import my_new_source  # noqa: F401
```

The new source will automatically appear in `sources`, `fetch`, and the dashboard.

## Data Freshness

Census and BLS data has a release lag:

| Source | Latest available | Next release |
|---|---|---|
| Census PEP (metros) | 2023 | ~Dec 2026 |
| Census ACS 1-year (cities) | 2024 | ~Sept 2026 |
| BLS QCEW (employment/wages) | 2024 | ~Q3 2026 |
| BLS LAUS (unemployment rate) | Through Dec 2025 | Monthly |

Re-run `fetch` periodically to pick up new releases.

---

Built with [Claude Code](https://claude.ai/claude-code) (Claude Opus 4.6).
