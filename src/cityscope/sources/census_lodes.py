"""Census LEHD LODES (Origin-Destination Employment Statistics).

Powers the OnTheMap visualization. We pull WAC (workplace) and RAC
(residence) Area Characteristics — block-level employment counts —
and aggregate them up to census tracts to get:

- jobs_in_tract (where jobs physically are)
- workers_living_in_tract (where workers sleep)
- jobs_workers_ratio (job-center vs bedroom-community signal)
- high-earner share at workplace and at residence
- young-worker share
- industry mix (goods-producing / trade-transport / other-services)

LODES is published as gzipped CSVs per state per year. Files are
50–200 MB compressed. The first lookup for a state takes 5–10 seconds
to download + parse; subsequent lookups for any tract in that state
hit the in-memory cache.

Files: https://lehd.ces.census.gov/data/lodes/LODES8/{state}/{wac|rac}/
"""

from __future__ import annotations

import csv
import gzip
import io
import logging
from collections import defaultdict
from datetime import datetime, timezone

import httpx

from ..core.config import Config
from ..core.models import (
    DataPoint,
    DatasetMetadata,
    FetchResult,
    GeoType,
    Geography,
)
from ..core.registry import SourceRegistry
from ..core.source import DataSource

logger = logging.getLogger(__name__)

LODES_BASE = "https://lehd.ces.census.gov/data/lodes/LODES8"
TIMEOUT = 300.0
USER_AGENT = "cityscope/0.4.0"

# Massachusetts opted out of the LEHD program — LODES has no data for MA.
LODES_UNAVAILABLE_STATES: set[str] = {"25"}

# 2-digit state FIPS → lowercase USPS abbreviation (LODES URL component).
STATE_FIPS_TO_POSTAL: dict[str, str] = {
    "01": "al", "02": "ak", "04": "az", "05": "ar", "06": "ca",
    "08": "co", "09": "ct", "10": "de", "11": "dc", "12": "fl",
    "13": "ga", "15": "hi", "16": "id", "17": "il", "18": "in",
    "19": "ia", "20": "ks", "21": "ky", "22": "la", "23": "me",
    "24": "md", "25": "ma", "26": "mi", "27": "mn", "28": "ms",
    "29": "mo", "30": "mt", "31": "ne", "32": "nv", "33": "nh",
    "34": "nj", "35": "nm", "36": "ny", "37": "nc", "38": "nd",
    "39": "oh", "40": "ok", "41": "or", "42": "pa", "44": "ri",
    "45": "sc", "46": "sd", "47": "tn", "48": "tx", "49": "ut",
    "50": "vt", "51": "va", "53": "wa", "54": "wv", "55": "wi",
    "56": "wy", "72": "pr",
}

# Industry sector rollups, per BLS supersector convention.
# Goods-Producing = Natural Resources & Mining + Construction + Manufacturing.
# Trade/Transport/Utilities is its own supersector; Utilities (CNS03) lives there,
# not in Goods (a common mistake — verified against BLS supersector definitions).
GOODS_INDUSTRIES = ["CNS01", "CNS02", "CNS04", "CNS05"]                     # Ag, Mining, Construction, Mfg
TRADE_INDUSTRIES = ["CNS03", "CNS06", "CNS07", "CNS08"]                     # Utilities, Wholesale, Retail, Transport/Warehousing
SERVICES_INDUSTRIES = [
    "CNS09", "CNS10", "CNS11", "CNS12", "CNS13", "CNS14",
    "CNS15", "CNS16", "CNS17", "CNS18", "CNS19", "CNS20",
]


# Module-level cache: (state_postal, year, file_type) -> { tract_geoid: { col: int } }
_CACHE: dict[tuple[str, int, str], dict[str, dict[str, int]]] = {}
_YEAR_CACHE: dict[str, int] = {}


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


def _detect_lodes_year(state_postal: str) -> int:
    """Probe recent years and return the latest LODES vintage available for this state."""
    if state_postal in _YEAR_CACHE:
        return _YEAR_CACHE[state_postal]

    current = datetime.now().year
    for year in range(current - 1, current - 6, -1):
        url = (
            f"{LODES_BASE}/{state_postal}/wac/"
            f"{state_postal}_wac_S000_JT00_{year}.csv.gz"
        )
        try:
            with httpx.Client(timeout=15.0, headers={"User-Agent": USER_AGENT}) as c:
                r = c.head(url)
                if r.status_code == 200:
                    _YEAR_CACHE[state_postal] = year
                    logger.info("Detected latest LODES year for %s: %d", state_postal, year)
                    return year
        except httpx.HTTPError:
            continue
    raise RuntimeError(f"Could not detect LODES year for state {state_postal}")


def _load_lodes_state(
    state_postal: str, year: int, file_type: str,
) -> dict[str, dict[str, int]]:
    """Download a state's LODES CSV (WAC or RAC), aggregate blocks → tracts.

    Cached in memory after first call.

    Returns: {tract_geoid: {column_name: aggregated_int}}
    """
    cache_key = (state_postal, year, file_type)
    if cache_key in _CACHE:
        return _CACHE[cache_key]

    url = (
        f"{LODES_BASE}/{state_postal}/{file_type}/"
        f"{state_postal}_{file_type}_S000_JT00_{year}.csv.gz"
    )
    logger.info("Downloading LODES %s for %s %d (~50–200 MB)", file_type, state_postal, year)

    with httpx.Client(timeout=TIMEOUT, headers={"User-Agent": USER_AGENT}) as client:
        resp = client.get(url, follow_redirects=True)
        resp.raise_for_status()

    # Decompress gzip, parse CSV in-memory
    text = gzip.decompress(resp.content).decode("utf-8")
    geocode_col = "w_geocode" if file_type == "wac" else "h_geocode"

    by_tract: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        block_geoid = (row.get(geocode_col) or "").zfill(15)
        if len(block_geoid) != 15:
            continue
        tract_geoid = block_geoid[:11]  # state(2) + county(3) + tract(6)

        for col, raw in row.items():
            if col in (geocode_col, "createdate") or raw is None:
                continue
            try:
                by_tract[tract_geoid][col] += int(raw)
            except (ValueError, TypeError):
                continue

    # Convert nested defaultdicts to plain dicts for caching.
    result = {tract: dict(metrics) for tract, metrics in by_tract.items()}
    logger.info(
        "Parsed LODES %s for %s %d: %d tracts",
        file_type, state_postal, year, len(result),
    )
    _CACHE[cache_key] = result
    return result


# ---------------------------------------------------------------------------
# Metric extraction
# ---------------------------------------------------------------------------


def _share(numerator: int, denominator: int) -> float | None:
    """Return numerator/denominator * 100, or None if denom is zero."""
    if denominator <= 0:
        return None
    return round(numerator / denominator * 100.0, 2)


def _extract_metrics(
    wac: dict[str, int],
    rac: dict[str, int],
) -> dict[str, float]:
    """Compute the user-facing metrics from a tract's WAC + RAC counters."""
    metrics: dict[str, float] = {}

    jobs = wac.get("C000", 0)
    workers = rac.get("C000", 0)

    # Headline counts
    if jobs > 0:
        metrics["jobs_in_tract"] = float(jobs)
    if workers > 0:
        metrics["workers_living_in_tract"] = float(workers)

    # Jobs vs workers (only if both sides are non-trivial — avoid noisy ratios on tiny tracts)
    if jobs >= 50 and workers >= 50:
        metrics["jobs_workers_ratio"] = round(jobs / workers, 2)

    # Workplace breakdowns
    if jobs > 0:
        if (high := _share(wac.get("CE03", 0), jobs)) is not None:
            metrics["jobs_high_earner_pct"] = high
        if (young := _share(wac.get("CA01", 0), jobs)) is not None:
            metrics["jobs_young_worker_pct"] = young

        goods = sum(wac.get(c, 0) for c in GOODS_INDUSTRIES)
        trade = sum(wac.get(c, 0) for c in TRADE_INDUSTRIES)
        services = sum(wac.get(c, 0) for c in SERVICES_INDUSTRIES)
        if (g := _share(goods, jobs)) is not None:
            metrics["jobs_industry_goods_pct"] = g
        if (t := _share(trade, jobs)) is not None:
            metrics["jobs_industry_trade_pct"] = t
        if (s := _share(services, jobs)) is not None:
            metrics["jobs_industry_services_pct"] = s

    # Residence breakdowns
    if workers > 0:
        if (high_res := _share(rac.get("CE03", 0), workers)) is not None:
            metrics["workers_high_earner_pct"] = high_res

    return metrics


# ---------------------------------------------------------------------------
# Source
# ---------------------------------------------------------------------------


@SourceRegistry.register
class CensusLODESSource(DataSource):
    source_id = "census_lodes"
    name = "Census LEHD LODES"
    description = (
        "Workplace and residence employment characteristics at the census tract "
        "level (jobs, workers, industry mix, earnings) — powers OnTheMap"
    )
    supported_geo_types_for_lookup = [GeoType.TRACT]

    def __init__(self, config: Config):
        super().__init__(config)

    def fetch(self, **kwargs) -> FetchResult:
        """Bulk fetch isn't supported (gigabytes of data nationwide).
        Use fetch_for_geo() per address-lookup instead.
        """
        return FetchResult(
            geographies=[],
            data_points=[],
            metadata=DatasetMetadata(
                source_id=self.source_id,
                name=self.name,
                description=self.description,
                metrics=[
                    "jobs_in_tract", "workers_living_in_tract", "jobs_workers_ratio",
                    "jobs_high_earner_pct", "workers_high_earner_pct",
                    "jobs_young_worker_pct",
                    "jobs_industry_goods_pct", "jobs_industry_trade_pct",
                    "jobs_industry_services_pct",
                ],
                geo_types=[GeoType.TRACT],
                min_year=0,
                max_year=0,
                last_fetched=datetime.now(timezone.utc),
                record_count=0,
            ),
        )

    def fetch_for_geo(self, geo_id: str, geo_type: GeoType) -> FetchResult:
        if geo_type != GeoType.TRACT:
            raise NotImplementedError(f"census_lodes does not support {geo_type}")
        if len(geo_id) != 11:
            raise ValueError(
                f"Tract GEOID must be 11 digits (state+county+tract), got {geo_id!r}"
            )

        state_fips = geo_id[:2]
        if state_fips in LODES_UNAVAILABLE_STATES:
            logger.info(
                "LODES unavailable for state FIPS %s (e.g., MA opted out)", state_fips,
            )
            return self._empty_result(year=0)

        state_postal = STATE_FIPS_TO_POSTAL.get(state_fips)
        if not state_postal:
            logger.warning("No LODES URL postal mapping for state FIPS %s", state_fips)
            return self._empty_result(year=0)

        try:
            year = _detect_lodes_year(state_postal)
        except RuntimeError as e:
            logger.warning("LODES year detection failed for %s: %s", state_postal, e)
            return self._empty_result(year=0)

        wac_data: dict[str, int] = {}
        rac_data: dict[str, int] = {}
        try:
            wac_data = _load_lodes_state(state_postal, year, "wac").get(geo_id, {})
            rac_data = _load_lodes_state(state_postal, year, "rac").get(geo_id, {})
        except httpx.HTTPError as e:
            logger.warning("LODES download failed for %s %d: %s", state_postal, year, e)
            return self._empty_result(year=year)

        metrics = _extract_metrics(wac_data, rac_data)
        now = datetime.now(timezone.utc)
        points = [
            DataPoint(
                geo_id=geo_id,
                metric=metric,
                year=year,
                value=value,
                source=self.source_id,
                vintage=f"lodes_{year}",
                fetched_at=now,
            )
            for metric, value in metrics.items()
        ]

        return FetchResult(
            geographies=[],
            data_points=points,
            metadata=DatasetMetadata(
                source_id=self.source_id,
                name=self.name,
                description=self.description,
                metrics=list(metrics.keys()),
                geo_types=[GeoType.TRACT],
                min_year=year,
                max_year=year,
                last_fetched=now,
                record_count=len(points),
            ),
        )

    def _empty_result(self, year: int) -> FetchResult:
        return FetchResult(
            geographies=[],
            data_points=[],
            metadata=DatasetMetadata(
                source_id=self.source_id,
                name=self.name,
                description=self.description,
                metrics=[],
                geo_types=[GeoType.TRACT],
                min_year=year,
                max_year=year,
                last_fetched=datetime.now(timezone.utc),
                record_count=0,
            ),
        )
