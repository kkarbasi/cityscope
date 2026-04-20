"""cityscope: Real estate investment research data pipeline.

Quick start:
    from cityscope import api

    api.fetch("census_population")
    df = api.to_dataframe(metric="population_change_pct", geo_type="metro", year=2024)
"""

from importlib.metadata import version as _get_version

__version__ = _get_version("cityscope")
