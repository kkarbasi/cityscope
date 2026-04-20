"""Public API for urban-research.

This module exposes a clean programmatic interface to the data pipeline.
Users who `pip install urban-research` interact through this API or the CLI.

Example usage:

    from urban_research import api

    # Fetch population data
    result = api.fetch("census_population")
    print(f"Fetched {result.metadata.record_count} data points")

    # Query stored data
    rows = api.query(metric="population_change_pct", geo_type="metro", year=2024, limit=10)
    for row in rows:
        print(f"{row['name']}: {row['value']:+.2f}%")

    # Get available sources
    sources = api.list_sources()

    # Get data as a pandas DataFrame
    df = api.to_dataframe(metric="employment_change_pct", geo_type="metro")
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from .core.config import Config
from .core.models import FetchResult
from .core.registry import SourceRegistry
from .core.storage import Storage
from .pipeline.runner import Pipeline

import urban_research.sources  # noqa: F401 — triggers registration


_config: Config | None = None
_storage: Storage | None = None


def configure(
    config_path: str | Path | None = None,
    db_path: str | Path | None = None,
    min_population: int | None = None,
    census_api_key: str | None = None,
    bls_api_key: str | None = None,
) -> Config:
    """Configure the library. Call before other functions, or use defaults.

    Args:
        config_path: Path to a YAML config file.
        db_path: Override database path.
        min_population: Override minimum population filter.
        census_api_key: Census Bureau API key.
        bls_api_key: BLS API key.

    Returns:
        The active Config object.
    """
    global _config, _storage

    if config_path:
        _config = Config.load(Path(config_path))
    else:
        _config = Config()

    if db_path:
        _config.storage.db_path = str(db_path)
    if min_population is not None:
        _config.pipeline.min_population = min_population
    if census_api_key:
        _config.census.api_key = census_api_key
    if bls_api_key:
        _config.bls.api_key = bls_api_key

    _storage = None  # reset cached storage
    return _config


def _get_config() -> Config:
    global _config
    if _config is None:
        _config = Config()
    return _config


def _get_storage() -> Storage:
    global _storage
    if _storage is None:
        _storage = Storage(_get_config().storage.db_path)
    return _storage


# ---------------------------------------------------------------------------
# Source discovery
# ---------------------------------------------------------------------------


def list_sources() -> list[dict[str, str]]:
    """List all registered data sources.

    Returns:
        List of dicts with keys: id, name, description.
    """
    result = []
    for source_id in SourceRegistry.list_ids():
        cls = SourceRegistry._sources[source_id]
        result.append({
            "id": source_id,
            "name": cls.name,
            "description": cls.description,
        })
    return result


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------


def fetch(
    source_id: str | None = None,
    *,
    all_sources: bool = False,
    **kwargs,
) -> dict[str, FetchResult]:
    """Fetch data from one or all sources and store it.

    Args:
        source_id: The source to fetch (e.g. "census_population").
        all_sources: If True, fetch from all registered sources.
        **kwargs: Passed to the source's fetch method (e.g. vintage, min_population, skip_laus).

    Returns:
        Dict mapping source_id to FetchResult.
    """
    config = _get_config()
    pipeline = Pipeline(config)

    if all_sources:
        ids = None
    elif source_id:
        ids = [source_id]
    else:
        raise ValueError("Provide source_id or set all_sources=True")

    return pipeline.run(ids, **kwargs)


# ---------------------------------------------------------------------------
# Querying
# ---------------------------------------------------------------------------


def query(
    metric: str | None = None,
    geo_type: str | None = None,
    source: str | None = None,
    year: int | None = None,
    min_population: int | None = None,
    limit: int = 1000,
) -> list[dict[str, Any]]:
    """Query stored data points.

    Args:
        metric: Filter by metric name (e.g. "population_change_pct").
        geo_type: Filter by geography type ("metro" or "city").
        source: Filter by data source.
        year: Filter by year.
        min_population: Minimum population threshold.
        limit: Max rows to return.

    Returns:
        List of dicts with keys: geo_id, metric, year, value, source,
        vintage, fetched_at, name, geo_type, population.
    """
    storage = _get_storage()
    return storage.query_data(
        metric=metric,
        geo_type=geo_type,
        source=source,
        year=year,
        min_population=min_population,
        limit=limit,
    )


def get_geographies(
    geo_type: str | None = None,
    min_population: int | None = None,
) -> list[dict[str, Any]]:
    """Get stored geographies.

    Args:
        geo_type: Filter by type ("metro" or "city").
        min_population: Minimum population threshold.

    Returns:
        List of geography dicts.
    """
    storage = _get_storage()
    return storage.get_geographies(geo_type=geo_type, min_population=min_population)


def status() -> list[dict[str, Any]]:
    """Get summary of fetched data.

    Returns:
        List of dicts with keys: source, metric, records, min_year, max_year, last_fetched.
    """
    storage = _get_storage()
    return storage.get_sources_summary()


# ---------------------------------------------------------------------------
# Pandas integration
# ---------------------------------------------------------------------------


def to_dataframe(
    metric: str | None = None,
    geo_type: str | None = None,
    source: str | None = None,
    year: int | None = None,
    min_population: int | None = None,
    limit: int = 50_000,
) -> pd.DataFrame:
    """Query stored data and return as a pandas DataFrame.

    Same arguments as query(). Returns a DataFrame with columns:
    geo_id, metric, year, value, source, name, geo_type, population.
    """
    rows = query(
        metric=metric,
        geo_type=geo_type,
        source=source,
        year=year,
        min_population=min_population,
        limit=limit,
    )
    return pd.DataFrame(rows)
