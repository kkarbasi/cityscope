"""Tests for hud_fmr source.

We mock the network + XLSX-parsing layer so tests don't need to download
HUD's files or handle openpyxl's parsing quirks.
"""

from unittest.mock import patch

import pytest

from cityscope.core.models import GeoType
from cityscope.sources import hud_fmr
from cityscope.sources.hud_fmr import HUDFMRSource


@pytest.fixture(autouse=True)
def clear_cache():
    """Reset the module-level FMR cache between tests."""
    hud_fmr._CACHE = None
    yield
    hud_fmr._CACHE = None


@pytest.fixture
def fake_fmr_table():
    """A fake parsed FMR table for 3 counties."""
    return {
        "fy": 2025,
        "by_county": {
            "06085": {
                "hud_area_name": "San Jose-Sunnyvale-Santa Clara, CA MSA",
                "fmr_studio": 2608.0,
                "fmr_1br": 2975.0,
                "fmr_2br": 3446.0,
                "fmr_3br": 4477.0,
                "fmr_4br": 4878.0,
            },
            "36061": {
                "hud_area_name": "New York, NY HUD Metro FMR Area",
                "fmr_studio": 2200.0,
                "fmr_1br": 2400.0,
                "fmr_2br": 2800.0,
                "fmr_3br": 3600.0,
                "fmr_4br": 4000.0,
            },
            "48113": {
                "hud_area_name": "Dallas, TX HUD Metro FMR Area",
                "fmr_studio": 1400.0,
                "fmr_1br": 1500.0,
                "fmr_2br": 1800.0,
                "fmr_3br": 2300.0,
                "fmr_4br": 2700.0,
            },
        },
        "source_url": "https://example.test/fake.xlsx",
    }


class TestHUDFMRSource:
    def test_metadata(self, config):
        source = HUDFMRSource(config)
        assert source.source_id == "hud_fmr"
        assert source.supported_geo_types_for_lookup == [GeoType.COUNTY]

    def test_fetch_for_geo_known_county(self, config, fake_fmr_table):
        source = HUDFMRSource(config)
        with patch("cityscope.sources.hud_fmr._load_fmr_table", return_value=fake_fmr_table):
            result = source.fetch_for_geo("06085", GeoType.COUNTY)

        metrics = {p.metric: p.value for p in result.data_points}
        assert metrics == {
            "fmr_studio": 2608.0,
            "fmr_1br": 2975.0,
            "fmr_2br": 3446.0,
            "fmr_3br": 4477.0,
            "fmr_4br": 4878.0,
        }
        # All data points should be the same year, same source
        years = {p.year for p in result.data_points}
        sources = {p.source for p in result.data_points}
        assert years == {2025}
        assert sources == {"hud_fmr"}

    def test_fetch_for_geo_unknown_county(self, config, fake_fmr_table):
        source = HUDFMRSource(config)
        with patch("cityscope.sources.hud_fmr._load_fmr_table", return_value=fake_fmr_table):
            result = source.fetch_for_geo("99999", GeoType.COUNTY)
        # Unknown counties return empty result, not an exception
        assert result.data_points == []

    def test_fetch_for_geo_rejects_non_county(self, config):
        source = HUDFMRSource(config)
        with pytest.raises(NotImplementedError):
            source.fetch_for_geo("41940", GeoType.METRO)

    def test_fetch_for_geo_rejects_bad_geo_id_length(self, config, fake_fmr_table):
        source = HUDFMRSource(config)
        with patch("cityscope.sources.hud_fmr._load_fmr_table", return_value=fake_fmr_table):
            with pytest.raises(ValueError, match="5 digits"):
                source.fetch_for_geo("123456", GeoType.COUNTY)

    def test_bulk_fetch(self, config, fake_fmr_table):
        """The bulk fetch() should return all counties' data in one FetchResult."""
        source = HUDFMRSource(config)
        with patch("cityscope.sources.hud_fmr._load_fmr_table", return_value=fake_fmr_table):
            result = source.fetch()

        # 3 counties × 5 metrics = 15 points
        assert len(result.data_points) == 15
        # Each county should appear
        geo_ids = {p.geo_id for p in result.data_points}
        assert geo_ids == {"06085", "36061", "48113"}
