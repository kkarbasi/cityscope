"""Tests for census_rents source."""

from unittest.mock import patch

import httpx
import pytest

from cityscope.core.config import Config
from cityscope.core.models import GeoType
from cityscope.sources.census_rents import (
    CensusRentsSource,
    NA_SENTINEL,
    _parse_rent_value,
    _parse_tract_geoid,
)


class TestParseTractGeoid:
    def test_split_11_digit_geoid(self):
        state, county, tract = _parse_tract_geoid("06085504601")
        assert state == "06"
        assert county == "085"
        assert tract == "504601"

    def test_rejects_wrong_length(self):
        with pytest.raises(ValueError, match="11 digits"):
            _parse_tract_geoid("06085")
        with pytest.raises(ValueError, match="11 digits"):
            _parse_tract_geoid("06085504601504601")


class TestParseRentValue:
    def test_valid_int(self):
        assert _parse_rent_value("2500") == 2500.0

    def test_valid_str_passthrough(self):
        assert _parse_rent_value(2500) == 2500.0

    def test_sentinel_returns_none(self):
        assert _parse_rent_value(NA_SENTINEL) is None
        assert _parse_rent_value(str(NA_SENTINEL)) is None

    def test_negative_returns_none(self):
        assert _parse_rent_value(-1) is None

    def test_garbage_returns_none(self):
        assert _parse_rent_value(None) is None
        assert _parse_rent_value("not a number") is None


class TestCensusRentsSource:
    def test_metadata(self, config):
        source = CensusRentsSource(config)
        assert source.source_id == "census_rents"
        assert GeoType.TRACT in source.supported_geo_types_for_lookup
        assert GeoType.COUNTY in source.supported_geo_types_for_lookup
        assert GeoType.CITY in source.supported_geo_types_for_lookup

    def test_fetch_for_geo_parses_tract_response(self, config):
        """With a mocked ACS response, verify the source produces
        well-formed DataPoints and handles the -666666666 sentinel."""
        source = CensusRentsSource(config)

        # Mock the vintage detection so no network call
        with patch.object(source, "_vintage", return_value=2023):
            # Mock the ACS API response
            fake_response_data = [
                ["NAME", "B25031_001E", "B25031_002E", "B25031_003E",
                 "B25031_004E", "B25031_005E", "B25031_006E", "B25031_007E",
                 "state", "county", "tract"],
                ["Census Tract 5046.01; Santa Clara County; California",
                 "2765", str(NA_SENTINEL), "2200", "2461",
                 str(NA_SENTINEL), str(NA_SENTINEL), str(NA_SENTINEL),
                 "06", "085", "504601"],
            ]

            fake_resp = httpx.Response(
                200,
                json=fake_response_data,
                request=httpx.Request("GET", "https://fake.invalid"),
            )

            with patch("cityscope.sources.census_rents._get", return_value=fake_resp):
                result = source.fetch_for_geo("06085504601", GeoType.TRACT)

        # Should produce: median_rent_all (2765), median_rent_1br (2200),
        # median_rent_2br (2461) — other bedrooms suppressed
        metrics = {p.metric: p.value for p in result.data_points}
        assert metrics.get("median_rent_all") == 2765.0
        assert metrics.get("median_rent_1br") == 2200.0
        assert metrics.get("median_rent_2br") == 2461.0
        # Suppressed values should be filtered out
        assert "median_rent_studio" not in metrics
        assert "median_rent_3br" not in metrics
        assert "median_rent_4br" not in metrics
        assert "median_rent_5br_plus" not in metrics

        # And it should yield a tract Geography
        assert len(result.geographies) == 1
        assert result.geographies[0].geo_type == GeoType.TRACT
        assert result.geographies[0].state_fips == "06"

    def test_fetch_for_geo_rejects_bad_tract_length(self, config):
        source = CensusRentsSource(config)
        with patch.object(source, "_vintage", return_value=2023):
            with pytest.raises(ValueError, match="11 digits"):
                source.fetch_for_geo("12345", GeoType.TRACT)

    def test_fetch_for_geo_unsupported_geo_type(self, config):
        source = CensusRentsSource(config)
        with patch.object(source, "_vintage", return_value=2023):
            with pytest.raises(NotImplementedError):
                source.fetch_for_geo("12345", GeoType.STATE)
