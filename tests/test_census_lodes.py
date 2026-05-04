"""Tests for census_lodes source."""

from unittest.mock import patch

import pytest

from cityscope.core.config import Config
from cityscope.core.models import GeoType
from cityscope.sources import census_lodes
from cityscope.sources.census_lodes import (
    CensusLODESSource,
    LODES_UNAVAILABLE_STATES,
    STATE_FIPS_TO_POSTAL,
    _extract_metrics,
    _share,
)


@pytest.fixture(autouse=True)
def clear_caches():
    """Reset module-level caches between tests."""
    census_lodes._CACHE.clear()
    census_lodes._YEAR_CACHE.clear()
    yield
    census_lodes._CACHE.clear()
    census_lodes._YEAR_CACHE.clear()


class TestStateMapping:
    def test_california_postal(self):
        assert STATE_FIPS_TO_POSTAL["06"] == "ca"

    def test_dc(self):
        assert STATE_FIPS_TO_POSTAL["11"] == "dc"

    def test_massachusetts_in_unavailable_list(self):
        assert "25" in LODES_UNAVAILABLE_STATES

    def test_all_50_states_plus_dc_pr_mapped(self):
        # 50 states + DC + PR = 52 entries
        assert len(STATE_FIPS_TO_POSTAL) == 52


class TestShare:
    def test_basic(self):
        assert _share(50, 100) == 50.0
        assert _share(33, 100) == 33.0

    def test_rounding(self):
        # 1/3 rounded to 2 decimal places
        assert _share(1, 3) == 33.33

    def test_zero_denominator_returns_none(self):
        assert _share(5, 0) is None

    def test_negative_denominator_returns_none(self):
        assert _share(5, -1) is None


class TestExtractMetrics:
    def test_typical_office_district(self):
        """Tract with lots of jobs, few residents (e.g., Googleplex)."""
        # Industries fully accounted for so percentages sum to 100.
        wac = {
            "C000": 40_000, "CE03": 38_000, "CA01": 5_000,
            "CNS01": 100, "CNS02": 0, "CNS03": 200, "CNS04": 50, "CNS05": 1_650,  # 2,000 goods
            "CNS06": 500, "CNS07": 1_300, "CNS08": 200,                            # 2,000 trade
            "CNS12": 32_000, "CNS09": 4_000,                                       # 36,000 services
        }
        rac = {"C000": 1_000, "CE03": 750}

        m = _extract_metrics(wac, rac)
        assert m["jobs_in_tract"] == 40_000.0
        assert m["workers_living_in_tract"] == 1_000.0
        assert m["jobs_workers_ratio"] == 40.0
        assert m["jobs_high_earner_pct"] == 95.0
        assert m["workers_high_earner_pct"] == 75.0
        assert m["jobs_industry_goods_pct"] == 5.0      # 2,000 / 40,000
        assert m["jobs_industry_trade_pct"] == 5.0      # 2,000 / 40,000
        assert m["jobs_industry_services_pct"] == 90.0  # 36,000 / 40,000

    def test_residential_only_tract(self):
        """No jobs, just workers commuting out."""
        wac = {"C000": 0}
        rac = {"C000": 5_000, "CE03": 2_000}
        m = _extract_metrics(wac, rac)
        assert "jobs_in_tract" not in m
        assert m["workers_living_in_tract"] == 5_000.0
        assert m["workers_high_earner_pct"] == 40.0
        # Ratio is suppressed because jobs is below 50
        assert "jobs_workers_ratio" not in m

    def test_empty_data_returns_empty_metrics(self):
        m = _extract_metrics({}, {})
        assert m == {}

    def test_ratio_suppressed_for_tiny_tracts(self):
        """Below 50 jobs OR 50 workers, ratio is too noisy to show."""
        wac = {"C000": 30}
        rac = {"C000": 100}
        m = _extract_metrics(wac, rac)
        assert m["jobs_in_tract"] == 30.0
        assert "jobs_workers_ratio" not in m

    def test_industry_rollup(self):
        """Industry rollup: CNS01-05 = goods, 06-08 = trade, 09-20 = services."""
        wac = {
            "C000": 1000,
            "CNS01": 100,  # Ag (goods)
            "CNS04": 100,  # Construction (goods)
            "CNS05": 100,  # Mfg (goods)
            "CNS07": 200,  # Retail (trade)
            "CNS08": 100,  # Transport (trade)
            "CNS12": 200,  # Professional (services)
            "CNS16": 200,  # Healthcare (services)
        }
        rac = {"C000": 1000, "CE03": 500}
        m = _extract_metrics(wac, rac)
        assert m["jobs_industry_goods_pct"] == 30.0   # 300/1000
        assert m["jobs_industry_trade_pct"] == 30.0   # 300/1000
        assert m["jobs_industry_services_pct"] == 40.0  # 400/1000


class TestCensusLODESSource:
    def test_metadata(self):
        source = CensusLODESSource(Config())
        assert source.source_id == "census_lodes"
        assert source.supported_geo_types_for_lookup == [GeoType.TRACT]

    def test_rejects_non_tract_geo_type(self):
        source = CensusLODESSource(Config())
        with pytest.raises(NotImplementedError):
            source.fetch_for_geo("06085", GeoType.COUNTY)

    def test_rejects_bad_tract_length(self):
        source = CensusLODESSource(Config())
        with pytest.raises(ValueError, match="11 digits"):
            source.fetch_for_geo("12345", GeoType.TRACT)

    def test_massachusetts_returns_empty(self):
        """MA is in LODES_UNAVAILABLE_STATES — should return empty cleanly."""
        source = CensusLODESSource(Config())
        result = source.fetch_for_geo("25025040600", GeoType.TRACT)  # tract in Boston
        assert result.data_points == []
        assert result.metadata.record_count == 0

    def test_unknown_state_fips_returns_empty(self):
        """Made-up state FIPS shouldn't crash — just empty."""
        source = CensusLODESSource(Config())
        result = source.fetch_for_geo("99001000000", GeoType.TRACT)
        assert result.data_points == []

    def test_full_extract_with_mocked_load(self):
        """Patch _load_lodes_state with fake data and verify metric extraction."""
        fake_wac = {
            "06085504601": {
                "C000": 40_000, "CE03": 38_000, "CA01": 5_000,
                "CNS01": 100, "CNS04": 50, "CNS05": 850,
                "CNS06": 500, "CNS07": 1_000, "CNS08": 200,
                "CNS12": 35_000, "CNS09": 2_300,
            }
        }
        fake_rac = {
            "06085504601": {"C000": 1_000, "CE03": 750},
        }

        def fake_load(state, year, ftype):
            return fake_wac if ftype == "wac" else fake_rac

        source = CensusLODESSource(Config())
        with patch.object(census_lodes, "_detect_lodes_year", return_value=2023), \
             patch.object(census_lodes, "_load_lodes_state", side_effect=fake_load):
            result = source.fetch_for_geo("06085504601", GeoType.TRACT)

        metrics_by_name = {p.metric: p.value for p in result.data_points}
        assert metrics_by_name["jobs_in_tract"] == 40_000.0
        assert metrics_by_name["workers_living_in_tract"] == 1_000.0
        assert metrics_by_name["jobs_workers_ratio"] == 40.0
        assert metrics_by_name["jobs_high_earner_pct"] == 95.0
        assert metrics_by_name["workers_high_earner_pct"] == 75.0
        assert all(p.year == 2023 for p in result.data_points)
        assert all(p.source == "census_lodes" for p in result.data_points)
