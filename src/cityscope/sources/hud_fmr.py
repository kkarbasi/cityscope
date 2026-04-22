"""HUD Fair Market Rents source.

Pulls county-level Fair Market Rents (FMRs) from HUD's annual XLSX release.
FMRs are HUD's 40th-percentile "gross rent including utilities" figure used
to set Section 8 voucher payment standards — a solid conservative floor
for investor underwriting.

We download the annual XLSX, cache the parsed table in memory, and look up
one county at a time via fetch_for_geo().

The XLSX has a malformed timestamp in its metadata that trips openpyxl,
so we patch the file in place before parsing.
"""

from __future__ import annotations

import io
import logging
import os
import re
import shutil
import tempfile
import zipfile
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

USER_AGENT = "cityscope/0.2.2"
TIMEOUT = 120.0

# HUD publishes a "revised" XLSX each fiscal year. Try newest first.
FMR_URL_CANDIDATES = [
    "https://www.huduser.gov/portal/datasets/fmr/fmr2025/FY25_FMRs_revised.xlsx",
    "https://www.huduser.gov/portal/datasets/fmr/fmr2025/FY25_FMRs.xlsx",
    "https://www.huduser.gov/portal/datasets/fmr/fmr2024/FMR2024_final_revised.xlsx",
    "https://www.huduser.gov/portal/datasets/fmr/fmr2024/FY24_FMRs.xlsx",
]

# Columns in the XLSX → our metric names
FMR_COLUMN_TO_METRIC = {
    "fmr_0": "fmr_studio",
    "fmr_1": "fmr_1br",
    "fmr_2": "fmr_2br",
    "fmr_3": "fmr_3br",
    "fmr_4": "fmr_4br",
}


def _patch_xlsx_core_xml(data: bytes) -> bytes:
    """HUD's XLSX has 'YYYY- M-DD' style dates in docProps/core.xml that
    break openpyxl's ISO8601 parser. Rewrite them to zero-padded form.
    """
    buf_in = io.BytesIO(data)
    buf_out = io.BytesIO()

    with zipfile.ZipFile(buf_in, "r") as z_in:
        with zipfile.ZipFile(buf_out, "w", zipfile.ZIP_DEFLATED) as z_out:
            for info in z_in.infolist():
                blob = z_in.read(info.filename)
                if info.filename == "docProps/core.xml":
                    text = blob.decode("utf-8", errors="replace")
                    text = re.sub(
                        r"(\d{4})-\s(\d)-",
                        lambda m: f"{m.group(1)}-0{m.group(2)}-",
                        text,
                    )
                    text = re.sub(r"T\s(\d):", lambda m: f"T0{m.group(1)}:", text)
                    blob = text.encode("utf-8")
                z_out.writestr(info, blob)

    return buf_out.getvalue()


def _download_fmr_xlsx() -> tuple[bytes, str, int]:
    """Download the latest FMR XLSX. Returns (bytes, url_used, fiscal_year)."""
    with httpx.Client(timeout=TIMEOUT, headers={"User-Agent": USER_AGENT},
                      follow_redirects=True) as client:
        last_err: Exception | None = None
        for url in FMR_URL_CANDIDATES:
            try:
                resp = client.get(url)
                if resp.status_code == 200:
                    # Extract FY from URL: ".../fmr2025/..." → 2025
                    m = re.search(r"fmr(\d{4})", url)
                    fy = int(m.group(1)) if m else datetime.now().year
                    logger.info("Downloaded HUD FMR from %s (FY%d)", url, fy)
                    return resp.content, url, fy
            except httpx.HTTPError as e:
                last_err = e
                continue
    raise RuntimeError(f"Could not download HUD FMR XLSX (last error: {last_err})")


# Module-level cache so we only download+parse once per process
_CACHE: dict | None = None


def _load_fmr_table() -> dict:
    """Download + parse the FMR XLSX once, cache as a dict keyed by 5-digit county FIPS.

    Returns:
        {
            "fy": int,
            "by_county": { "06085": {"fmr_0": 2000, "fmr_1": 2200, ...}, ... },
        }
    """
    global _CACHE
    if _CACHE is not None:
        return _CACHE

    raw, url, fy = _download_fmr_xlsx()
    patched = _patch_xlsx_core_xml(raw)

    # Write to a temp file so pandas can open it
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        tmp.write(patched)
        tmp_path = tmp.name

    try:
        # Lazy import pandas — only needed here
        import pandas as pd
        df = pd.read_excel(tmp_path, sheet_name=0, engine="openpyxl")
    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass

    # FIPS column is a float/int in Excel; normalize to 10-digit string,
    # then take the first 5 (state+county) as our key.
    by_county: dict[str, dict] = {}

    for _, row in df.iterrows():
        raw_fips = row.get("fips")
        if raw_fips is None or (isinstance(raw_fips, float) and pd.isna(raw_fips)):
            continue

        fips_str = str(int(raw_fips)).zfill(10)
        subcounty = fips_str[5:]
        # Only county-level rows (subcounty == '99999').
        # New England "town" rows (other subcounty codes) are skipped — we'd
        # need a sub-county geo_id from the geocoder to disambiguate, which
        # we don't track yet.
        if subcounty != "99999":
            continue

        county_geo_id = fips_str[:5]

        entry: dict[str, float | str] = {"hud_area_name": row.get("hud_area_name", "")}
        for col, metric in FMR_COLUMN_TO_METRIC.items():
            val = row.get(col)
            if val is None or (isinstance(val, float) and pd.isna(val)):
                continue
            try:
                entry[metric] = float(val)
            except (ValueError, TypeError):
                continue

        if any(k in entry for k in FMR_COLUMN_TO_METRIC.values()):
            by_county[county_geo_id] = entry

    logger.info("Parsed %d counties from HUD FMR FY%d", len(by_county), fy)
    _CACHE = {"fy": fy, "by_county": by_county, "source_url": url}
    return _CACHE


@SourceRegistry.register
class HUDFMRSource(DataSource):
    source_id = "hud_fmr"
    name = "HUD Fair Market Rents"
    description = "County-level annual Fair Market Rents from HUD (0BR–4BR)"
    supported_geo_types_for_lookup = [GeoType.COUNTY]

    def __init__(self, config: Config):
        super().__init__(config)

    def fetch(self, **kwargs) -> FetchResult:
        """Bulk-load the FMR table and return all counties as data points.

        This populates the DB for offline lookup; individual address lookups
        can also use fetch_for_geo() which hits the same cache.
        """
        table = _load_fmr_table()
        fy = table["fy"]
        now = datetime.now(timezone.utc)

        all_points: list[DataPoint] = []
        for county_geo_id, entry in table["by_county"].items():
            for col, metric in FMR_COLUMN_TO_METRIC.items():
                if metric not in entry:
                    continue
                all_points.append(DataPoint(
                    geo_id=county_geo_id,
                    metric=metric,
                    year=fy,
                    value=float(entry[metric]),
                    source=self.source_id,
                    vintage=f"hud_fmr_fy{fy}",
                    fetched_at=now,
                ))

        return FetchResult(
            geographies=[],
            data_points=all_points,
            metadata=DatasetMetadata(
                source_id=self.source_id,
                name=self.name,
                description=self.description,
                metrics=list(FMR_COLUMN_TO_METRIC.values()),
                geo_types=[GeoType.COUNTY],
                min_year=fy,
                max_year=fy,
                last_fetched=now,
                record_count=len(all_points),
            ),
        )

    def fetch_for_geo(self, geo_id: str, geo_type: GeoType) -> FetchResult:
        if geo_type != GeoType.COUNTY:
            raise NotImplementedError(f"hud_fmr does not support {geo_type}")
        if len(geo_id) != 5:
            raise ValueError(f"County GEOID must be 5 digits, got {geo_id!r}")

        table = _load_fmr_table()
        fy = table["fy"]
        entry = table["by_county"].get(geo_id)
        if entry is None:
            return FetchResult(
                geographies=[],
                data_points=[],
                metadata=DatasetMetadata(
                    source_id=self.source_id,
                    name=self.name,
                    description=self.description,
                    metrics=[],
                    geo_types=[GeoType.COUNTY],
                    min_year=fy,
                    max_year=fy,
                    last_fetched=datetime.now(timezone.utc),
                ),
            )

        now = datetime.now(timezone.utc)
        points: list[DataPoint] = []
        for col, metric in FMR_COLUMN_TO_METRIC.items():
            if metric not in entry:
                continue
            points.append(DataPoint(
                geo_id=geo_id,
                metric=metric,
                year=fy,
                value=float(entry[metric]),
                source=self.source_id,
                vintage=f"hud_fmr_fy{fy}",
                fetched_at=now,
            ))

        return FetchResult(
            geographies=[],
            data_points=points,
            metadata=DatasetMetadata(
                source_id=self.source_id,
                name=self.name,
                description=self.description,
                metrics=sorted({p.metric for p in points}),
                geo_types=[GeoType.COUNTY],
                min_year=fy,
                max_year=fy,
                last_fetched=now,
                record_count=len(points),
            ),
        )
