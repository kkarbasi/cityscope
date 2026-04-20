"""urban-research: Real estate investment research data pipeline.

Quick start:
    from urban_research import api

    api.fetch("census_population")
    df = api.to_dataframe(metric="population_change_pct", geo_type="metro", year=2024)
"""

__version__ = "0.1.0"
