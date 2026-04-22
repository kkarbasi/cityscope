"""Census ACS 5-year rent source.

Pulls median gross rent by number of bedrooms from ACS table B25031 at
the census tract level — perfect for "rent in the vicinity of an address".

Table: B25031 — Median Gross Rent by Number of Bedrooms
Variables:
  B25031_001E = Total (all bedroom counts)
  B25031_002E = No bedroom (studio)
  B25031_003E = 1 bedroom
  B25031_004E = 2 bedrooms
  B25031_005E = 3 bedrooms
  B25031_006E = 4 bedrooms
  B25031_007E = 5 or more bedrooms
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

import httpx

from ..core.config import Config
from ..core.models import (
    DataPoint,
    DatasetMetadata,
    FetchResult,
    Geography,
    GeoType,
)
from ..core.registry import SourceRegistry
from ..core.source import DataSource

logger = logging.getLogger(__name__)

CENSUS_BASE = "https://api.census.gov/data"
TIMEOUT = 60.0
MAX_RETRIES = 3
RETRY_DELAY = 2.0
USER_AGENT = "cityscope/0.2.2"

# Census "not applicable / suppressed" sentinel
NA_SENTINEL = -666666666

# ACS B25031 variables → metric name
RENT_VARIABLES: dict[str, str] = {
    "B25031_001E": "median_rent_all",
    "B25031_002E": "median_rent_studio",
    "B25031_003E": "median_rent_1br",
    "B25031_004E": "median_rent_2br",
    "B25031_005E": "median_rent_3br",
    "B25031_006E": "median_rent_4br",
    "B25031_007E": "median_rent_5br_plus",
}


def _get(url: str, params: dict[str, str]) -> httpx.Response:
    for attempt in range(MAX_RETRIES):
        try:
            with httpx.Client(timeout=TIMEOUT, headers={"User-Agent": USER_AGENT}) as client:
                resp = client.get(url, params=params)
                if resp.status_code == 200:
                    return resp
                if resp.status_code >= 500:
                    logger.warning("ACS API %d, retry %d/%d", resp.status_code, attempt + 1, MAX_RETRIES)
                    time.sleep(RETRY_DELAY * (attempt + 1))
                    continue
                resp.raise_for_status()
                return resp
        except httpx.TimeoutException:
            logger.warning("ACS API timeout, retry %d/%d", attempt + 1, MAX_RETRIES)
            time.sleep(RETRY_DELAY * (attempt + 1))
    raise RuntimeError(f"ACS API failed after {MAX_RETRIES} retries: {url}")


def _detect_acs5_vintage(api_key: str | None) -> int:
    """Probe recent ACS 5-year vintages and return the newest available."""
    current_year = datetime.now().year
    for year in range(current_year - 1, current_year - 5, -1):
        url = f"{CENSUS_BASE}/{year}/acs/acs5"
        params: dict[str, str] = {"get": "NAME", "for": "state:06"}
        if api_key:
            params["key"] = api_key
        try:
            resp = _get(url, params)
            if resp.status_code == 200:
                logger.info("Detected latest ACS 5-year vintage: %d", year)
                return year
        except Exception:
            continue
    raise RuntimeError("Could not detect ACS 5-year vintage")


def _parse_tract_geoid(tract_geoid: str) -> tuple[str, str, str]:
    """Split an 11-digit tract GEOID into (state_fips, county_fips, tract_code)."""
    if len(tract_geoid) != 11:
        raise ValueError(
            f"Tract GEOID must be 11 digits (state+county+tract), got {tract_geoid!r}"
        )
    return tract_geoid[:2], tract_geoid[2:5], tract_geoid[5:11]


def _parse_rent_value(raw: str | int | None) -> float | None:
    """Parse an ACS rent value, treating sentinels as missing."""
    if raw is None:
        return None
    try:
        val = int(raw)
    except (ValueError, TypeError):
        return None
    if val == NA_SENTINEL or val < 0:
        return None
    return float(val)


def _fetch_acs_rent(
    geo_type: GeoType,
    geo_id: str,
    vintage: int,
    api_key: str | None,
) -> tuple[Geography | None, list[DataPoint]]:
    """Fetch median rent by bedroom for a single geography from ACS 5-year."""
    get_vars = ",".join(["NAME"] + list(RENT_VARIABLES.keys()))

    params: dict[str, str] = {"get": get_vars}
    if api_key:
        params["key"] = api_key

    if geo_type == GeoType.TRACT:
        state_fips, county_fips, tract_code = _parse_tract_geoid(geo_id)
        params["for"] = f"tract:{tract_code}"
        params["in"] = f"state:{state_fips} county:{county_fips}"
    elif geo_type == GeoType.COUNTY:
        if len(geo_id) != 5:
            raise ValueError(f"County GEOID must be 5 digits, got {geo_id!r}")
        params["for"] = f"county:{geo_id[2:]}"
        params["in"] = f"state:{geo_id[:2]}"
    elif geo_type == GeoType.CITY:
        if len(geo_id) != 7:
            raise ValueError(f"City GEOID must be 7 digits, got {geo_id!r}")
        params["for"] = f"place:{geo_id[2:]}"
        params["in"] = f"state:{geo_id[:2]}"
    else:
        raise NotImplementedError(f"census_rents does not support {geo_type}")

    url = f"{CENSUS_BASE}/{vintage}/acs/acs5"
    logger.info("ACS 5-year rent fetch: %s %s (vintage %d)", geo_type.value, geo_id, vintage)
    data = _get(url, params).json()
    if len(data) < 2:
        return None, []

    headers = data[0]
    row = data[1]
    rec = dict(zip(headers, row))

    name = rec.get("NAME", "")
    now = datetime.now(timezone.utc)

    # Construct Geography (only for tract — metro/city/county already exist)
    geo: Geography | None = None
    if geo_type == GeoType.TRACT:
        geo = Geography(
            geo_id=geo_id,
            name=name,
            geo_type=GeoType.TRACT,
            state_fips=geo_id[:2],
        )

    # Extract rent metrics
    points: list[DataPoint] = []
    for var, metric in RENT_VARIABLES.items():
        value = _parse_rent_value(rec.get(var))
        if value is None:
            continue
        points.append(DataPoint(
            geo_id=geo_id,
            metric=metric,
            year=vintage,
            value=value,
            source="census_rents",
            vintage=f"acs5_{vintage}",
            fetched_at=now,
        ))

    return geo, points


@SourceRegistry.register
class CensusRentsSource(DataSource):
    source_id = "census_rents"
    name = "Census ACS 5-year Median Gross Rent"
    description = "Tract-level median rent by number of bedrooms (ACS table B25031)"
    supported_geo_types_for_lookup = [GeoType.TRACT, GeoType.COUNTY, GeoType.CITY]

    def __init__(self, config: Config):
        super().__init__(config)
        self._api_key = config.census.api_key
        self._vintage_cache: int | None = None

    def _vintage(self) -> int:
        if self._vintage_cache is None:
            self._vintage_cache = _detect_acs5_vintage(self._api_key)
        return self._vintage_cache

    def fetch(self, **kwargs) -> FetchResult:
        """Bulk fetch is not used for this source — rents are fetched per-address
        via fetch_for_geo() as part of the lookup flow. We return an empty result.
        """
        vintage = kwargs.get("vintage") or self._vintage()
        return FetchResult(
            geographies=[],
            data_points=[],
            metadata=DatasetMetadata(
                source_id=self.source_id,
                name=self.name,
                description=self.description,
                metrics=list(RENT_VARIABLES.values()),
                geo_types=self.supported_geo_types_for_lookup,
                min_year=vintage,
                max_year=vintage,
                last_fetched=datetime.now(timezone.utc),
                record_count=0,
            ),
        )

    def fetch_for_geo(self, geo_id: str, geo_type: GeoType) -> FetchResult:
        vintage = self._vintage()
        geo, points = _fetch_acs_rent(geo_type, geo_id, vintage, self._api_key)

        return FetchResult(
            geographies=[geo] if geo is not None else [],
            data_points=points,
            metadata=DatasetMetadata(
                source_id=self.source_id,
                name=self.name,
                description=self.description,
                metrics=list({p.metric for p in points}),
                geo_types=[geo_type],
                min_year=vintage,
                max_year=vintage,
                last_fetched=datetime.now(timezone.utc),
                record_count=len(points),
            ),
        )
