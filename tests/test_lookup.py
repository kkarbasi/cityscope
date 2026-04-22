"""Integration tests for api.lookup() with mocked geocoder."""

from unittest.mock import MagicMock, patch

import pytest

from cityscope import api
from cityscope.core.models import (
    DatasetMetadata,
    FetchResult,
    GeoType,
    LocationReport,
)
from cityscope.core.storage import Storage
from cityscope.geocoding import GeocodingError, GeocodingResult


@pytest.fixture(autouse=True)
def reset_api_state():
    api._config = None
    api._storage = None
    yield
    api._config = None
    api._storage = None


@pytest.fixture
def mountain_view_geocoding():
    """Fake geocoder result for 1600 Amphitheatre Pkwy."""
    return GeocodingResult(
        address="1600 Amphitheatre Pkwy, Mountain View, CA",
        matched_address="1600 AMPHITHEATRE PKWY, MOUNTAIN VIEW, CA, 94043",
        latitude=37.423120,
        longitude=-122.083521,
        state_fips="06",
        county_fips="085",
        county_geo_id="06085",
        place_fips="49670",
        place_geo_id="0649670",
        cbsa_code="41940",
        tract_geoid="06085504601",
    )


@pytest.fixture
def rural_geocoding():
    """Fake geocoder result for a rural address with no metro/place."""
    return GeocodingResult(
        address="rural route, middle of nowhere",
        matched_address="RURAL ROUTE",
        latitude=34.0,
        longitude=-107.0,
        state_fips="35",
        county_fips="051",
        county_geo_id="35051",
        place_fips=None,
        place_geo_id=None,
        cbsa_code=None,
        tract_geoid=None,
    )


class TestLookupReturnsReport:
    def test_empty_db_returns_report_with_warnings(self, tmp_db, mountain_view_geocoding):
        api.configure(db_path=tmp_db)
        with patch("cityscope.api.geocode_address", return_value=mountain_view_geocoding):
            report = api.lookup("1600 Amphitheatre Pkwy, Mountain View, CA")

        assert isinstance(report, LocationReport)
        assert report.matched_address == "1600 AMPHITHEATRE PKWY, MOUNTAIN VIEW, CA, 94043"
        assert report.state_fips == "06"
        assert report.metro is None
        assert report.city is None
        assert report.county is None
        assert report.tract is None
        assert len(report.warnings) == 4  # metro, city, county, tract all missing

    def test_report_populated_from_db(self, tmp_db, mountain_view_geocoding, sample_geographies, sample_data_points):
        """If DB has matching data, report should include it."""
        # Inject geographies matching the geocoder output
        from cityscope.core.models import DataPoint, Geography

        api.configure(db_path=tmp_db)
        storage = Storage(tmp_db)

        # Metro with matching CBSA
        metros = [Geography(
            geo_id="41940", name="San Jose-Sunnyvale-Santa Clara, CA Metro Area",
            geo_type=GeoType.METRO, population=1_945_767,
        )]
        storage.upsert_geographies(metros)
        storage.upsert_data_points([
            DataPoint(geo_id="41940", metric="employment", year=2024,
                      value=1_134_612.0, source="bls_employment"),
            DataPoint(geo_id="41940", metric="avg_annual_pay", year=2024,
                      value=194_865.0, source="bls_employment"),
        ])

        with patch("cityscope.api.geocode_address", return_value=mountain_view_geocoding):
            report = api.lookup("1600 Amphitheatre Pkwy, Mountain View, CA")

        assert report.metro is not None
        assert report.metro.geo_id == "41940"
        assert report.metro.year == 2024
        assert report.metro.metrics["employment"] == 1_134_612.0
        assert report.metro.metrics["avg_annual_pay"] == 194_865.0
        # City and county are still missing
        assert report.city is None
        assert report.county is None


class TestLookupAutoFetch:
    def test_auto_fetch_calls_sources(self, tmp_db, mountain_view_geocoding):
        """When auto_fetch=True and data is missing, sources' fetch_for_geo is called."""
        api.configure(db_path=tmp_db)

        # Mock a source that supports METRO lookup
        mock_source = MagicMock()
        mock_source.supported_geo_types_for_lookup = [GeoType.METRO, GeoType.CITY, GeoType.COUNTY]

        from cityscope.core.models import Geography, DataPoint

        def fake_fetch_for_geo(geo_id, geo_type):
            # Pretend to fetch something for any geo_type
            geo = Geography(
                geo_id=geo_id,
                name=f"Fake {geo_type.value} {geo_id}",
                geo_type=geo_type,
                population=100_000,
            )
            dp = DataPoint(
                geo_id=geo_id, metric="population", year=2024,
                value=100_000.0, source="fake",
            )
            return FetchResult(
                geographies=[geo],
                data_points=[dp],
                metadata=DatasetMetadata(
                    source_id="fake", name="Fake", description="",
                    metrics=["population"], geo_types=[geo_type],
                    min_year=2024, max_year=2024, record_count=1,
                ),
            )

        mock_source.fetch_for_geo = fake_fetch_for_geo

        with patch("cityscope.api.geocode_address", return_value=mountain_view_geocoding), \
             patch("cityscope.api.SourceRegistry") as mock_registry:
            mock_registry.list_ids.return_value = ["fake"]
            mock_registry.get.return_value = mock_source

            report = api.lookup(
                "1600 Amphitheatre Pkwy, Mountain View, CA",
                auto_fetch=True,
            )

        assert report.metro is not None
        assert report.metro.geo_id == "41940"
        assert report.city is not None
        assert report.city.geo_id == "0649670"
        assert report.county is not None
        assert report.county.geo_id == "06085"

    def test_without_auto_fetch_leaves_empty(self, tmp_db, mountain_view_geocoding):
        api.configure(db_path=tmp_db)
        with patch("cityscope.api.geocode_address", return_value=mountain_view_geocoding):
            report = api.lookup(
                "1600 Amphitheatre Pkwy, Mountain View, CA",
                auto_fetch=False,
            )
        # Empty DB, no auto-fetch → everything missing
        assert report.metro is None
        assert report.warnings


class TestLookupEdgeCases:
    def test_rural_address_warns_about_missing_metro_and_city(
        self, tmp_db, rural_geocoding,
    ):
        api.configure(db_path=tmp_db)
        with patch("cityscope.api.geocode_address", return_value=rural_geocoding):
            report = api.lookup("rural route")

        # Rural address has no CBSA or place, so these warnings fire
        warning_text = " ".join(report.warnings)
        assert "metro" in warning_text.lower()
        assert "city" in warning_text.lower()

    def test_geocoding_failure_propagates(self, tmp_db):
        api.configure(db_path=tmp_db)
        with patch(
            "cityscope.api.geocode_address",
            side_effect=GeocodingError("No match"),
        ):
            with pytest.raises(GeocodingError):
                api.lookup("invalid address")
