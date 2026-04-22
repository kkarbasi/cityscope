"""Public API for urban-research.

This module exposes a clean programmatic interface to the data pipeline.
Users who `pip install urban-research` interact through this API or the CLI.

Example usage:

    from cityscope import api

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
from .core.models import (
    FetchResult,
    GeoLevelSnapshot,
    GeoType,
    LocationReport,
)
from .core.registry import SourceRegistry
from .core.storage import Storage
from .geocoding import GeocodingError, GeocodingResult, geocode_address
from .pipeline.runner import Pipeline

import cityscope.sources  # noqa: F401 — triggers registration


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


# ---------------------------------------------------------------------------
# Address lookup
# ---------------------------------------------------------------------------


def _build_snapshot(
    storage: Storage,
    geo_id: str,
    geo_type: GeoType,
    year: int | None,
) -> GeoLevelSnapshot | None:
    """Build a GeoLevelSnapshot from stored data for a single geography.

    If `year` is not specified, picks the latest available year PER METRIC
    (so metrics from different sources with different release cadences all
    show up, each at their own most recent vintage).
    """
    rows = storage.query_data(
        geo_type=geo_type.value,
        year=year,
        limit=10_000,
    )
    rows = [r for r in rows if r["geo_id"] == geo_id]

    if not rows:
        return None

    if year is not None:
        year_rows = [r for r in rows if r["year"] == year]
        metrics = {r["metric"]: r["value"] for r in year_rows}
        report_year = year
    else:
        # For each metric, keep only the row for its latest year.
        latest_per_metric: dict[str, dict] = {}
        for r in rows:
            metric = r["metric"]
            current = latest_per_metric.get(metric)
            if current is None or r["year"] > current["year"]:
                latest_per_metric[metric] = r
        metrics = {m: row["value"] for m, row in latest_per_metric.items()}
        # Report year = most recent metric's year (mostly for display)
        report_year = max(row["year"] for row in latest_per_metric.values())

    if not metrics:
        return None

    first = rows[0]
    return GeoLevelSnapshot(
        geo_id=geo_id,
        name=first["name"],
        geo_type=geo_type,
        population=first.get("population"),
        year=report_year,
        metrics=metrics,
    )


def _try_fetch_for_geo(
    config: Config,
    geo_id: str,
    geo_type: GeoType,
) -> bool:
    """Try each registered source that supports this geo_type, skipping any
    source whose data is already in storage for this geo_id.

    Returns True if any source produced new data points.
    """
    any_success = False
    storage = _get_storage()

    for source_id in SourceRegistry.list_ids():
        source = SourceRegistry.get(source_id, config)
        if geo_type not in source.supported_geo_types_for_lookup:
            continue

        # Skip if we already have data from this source for this geo_id.
        existing = storage.query_data(source=source_id, limit=10_000)
        if any(r["geo_id"] == geo_id for r in existing):
            continue

        try:
            result = source.fetch_for_geo(geo_id, geo_type)
        except NotImplementedError:
            continue
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(
                "Fallback fetch failed for %s/%s via %s: %s",
                geo_type, geo_id, source_id, e,
            )
            continue

        if result.geographies:
            storage.upsert_geographies(result.geographies)
        if result.data_points:
            storage.upsert_data_points(result.data_points)
            any_success = True

    return any_success


def lookup(
    address: str,
    *,
    auto_fetch: bool = False,
    year: int | None = None,
) -> LocationReport:
    """Look up stats for a US address.

    Geocodes the address via the Census Geocoder, then assembles a report
    with metro-, city-, and county-level stats from local storage. If a
    given level has no local data and `auto_fetch=True`, any registered
    source that supports that geography type will be called as a fallback.

    Args:
        address: Free-form US address, e.g. "1600 Amphitheatre Pkwy, Mountain View, CA".
        auto_fetch: If True, fetch missing data from source APIs on-the-fly
                    and store it for future lookups. Default False.
        year: Year to report. If None, uses the latest available per level.

    Returns:
        LocationReport with metro/city/county snapshots (None if no data).

    Raises:
        GeocodingError: If the address cannot be matched.
    """
    config = _get_config()
    storage = _get_storage()

    geo_result: GeocodingResult = geocode_address(address)

    report = LocationReport(
        address=address,
        matched_address=geo_result.matched_address,
        latitude=geo_result.latitude,
        longitude=geo_result.longitude,
        state_fips=geo_result.state_fips,
        tract_geoid=geo_result.tract_geoid,
    )

    levels: list[tuple[str, GeoType, str | None]] = [
        ("metro", GeoType.METRO, geo_result.cbsa_code),
        ("city", GeoType.CITY, geo_result.place_geo_id),
        ("county", GeoType.COUNTY, geo_result.county_geo_id),
        ("tract", GeoType.TRACT, geo_result.tract_geoid),
    ]

    for field_name, geo_type, geo_id in levels:
        if not geo_id:
            report.warnings.append(
                f"No {geo_type.value} identifier for this address"
            )
            continue

        # When auto_fetch is on, try every source that supports this geo_type
        # and hasn't yet stored data for it — this enriches partial snapshots
        # (e.g., adding HUD FMR to a county that already has population data).
        if auto_fetch:
            _try_fetch_for_geo(config, geo_id, geo_type)

        snapshot = _build_snapshot(storage, geo_id, geo_type, year)

        if snapshot is None:
            msg = (
                f"No data available for {geo_type.value} {geo_id}"
                if auto_fetch
                else f"No local data for {geo_type.value} {geo_id} "
                     f"(re-run with auto_fetch=True to fetch from source)"
            )
            report.warnings.append(msg)

        setattr(report, field_name, snapshot)

    return report
