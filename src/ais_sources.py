"""AIS data source abstraction.

NOAA MarineCadastre publishes daily AIS pings, but the on-disk layout changes over
time: the current publishing year ships as ``ais-YYYY-MM-DD.csv.zst`` (streamable,
lowercase/dashed), while older years get archived into per-day
``AIS_YYYY_MM_DD.zip`` bundles (uppercase/underscored, larger). Publication also
lags real time by months, so "yesterday" is not a safe default — callers must
probe for what is actually available.

This module defines a minimal ``AISSource`` protocol so the ingest pipeline
(KDTree port filter + MotherDuck upload) stays source-agnostic, plus the first
concrete implementation, ``NoaaMarineCadastreSource``.
"""

from __future__ import annotations

import io
import logging
import re
import zipfile
from datetime import date, datetime, timezone
from typing import Protocol

import polars as pl
import requests
import zstandard as zstd

logger = logging.getLogger("ais_ingest")

# Normalized schema every AISSource.fetch() must return (subset/order not enforced,
# but these columns must be present).
NORMALIZED_COLUMNS = ["mmsi", "imo", "vessel_name", "latitude", "longitude", "base_date_time"]


class AISSource(Protocol):
    """Minimal contract for a pluggable AIS data source."""

    def available_dates(self, since: date, until: date) -> list[date]:
        """Return the sorted list of dates known to be published in [since, until]."""
        ...

    def latest_available_date(self, not_after: date | None = None) -> date | None:
        """Return the most recent published date at or before ``not_after`` (default: today UTC)."""
        ...

    def fetch(self, target_date: date) -> pl.DataFrame | None:
        """Fetch and normalize one day of AIS pings. Returns None if unavailable or unparsable."""
        ...


class NoaaMarineCadastreSource:
    """AISSource backed by NOAA's Marine Cadastre AIS Data Handler.

    Layout (probed live 2026-07-25):
      - Current/recent year (e.g. 2025): one file per day,
        ``{BASE}/{year}/ais-{YYYY-MM-DD}.csv.zst`` — snake_case columns already
        matching our normalized schema (mmsi, base_date_time, latitude,
        longitude, vessel_name, imo, ...).
      - Archived older years (e.g. 2023, 2024): one file per day,
        ``{BASE}/{year}/AIS_{YYYY}_{MM}_{DD}.zip`` containing a single CSV with
        the legacy Marine Cadastre schema (MMSI, BaseDateTime, LAT, LON,
        VesselName, IMO, ...), which we rename to match.
      - A year directory 404s entirely once nothing has been published for it
        yet (observed for 2026 as of 2026-07-25 — publication lags ~7 months).

    Both patterns are discovered by parsing each year's index page (an HTML
    directory listing), which is far cheaper than day-by-day HEAD probing.
    """

    BASE_URL = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler"
    _ZST_RE = re.compile(r"ais-(\d{4})-(\d{2})-(\d{2})\.csv\.zst")
    _ZIP_RE = re.compile(r"AIS_(\d{4})_(\d{2})_(\d{2})\.zip")

    def __init__(
        self,
        session: requests.Session | None = None,
        timeout: int = 30,
        retries: int = 3,
    ) -> None:
        self._session = session or requests.Session()
        self._timeout = timeout
        self._retries = retries
        self._year_index_cache: dict[int, dict[date, str]] = {}

    def _get(self, url: str, stream: bool = False) -> requests.Response | None:
        for attempt in range(self._retries):
            try:
                resp = self._session.get(url, stream=stream, timeout=self._timeout)
                if resp.status_code == 200:
                    return resp
                if resp.status_code == 404:
                    return None
                logger.warning(
                    f"Attempt {attempt + 1}/{self._retries} failed for {url}: "
                    f"Status {resp.status_code}"
                )
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1}/{self._retries} error for {url}: {e}")
        logger.error(f"Failed to fetch {url} after {self._retries} attempts.")
        return None

    def _year_index(self, year: int) -> dict[date, str]:
        """Parse a year's directory listing into {date: 'zst'|'zip'}. Cached per instance."""
        if year in self._year_index_cache:
            return self._year_index_cache[year]

        url = f"{self.BASE_URL}/{year}/"
        resp = self._get(url)
        mapping: dict[date, str] = {}
        if resp is not None:
            text = resp.text
            for m in self._ZST_RE.finditer(text):
                d = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
                mapping[d] = "zst"
            for m in self._ZIP_RE.finditer(text):
                d = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
                mapping.setdefault(d, "zip")
            if not mapping:
                logger.warning(f"NOAA year index {url} returned 200 but no recognizable files.")
        else:
            logger.info(f"NOAA year index {url} not found (year not yet published or too old).")

        self._year_index_cache[year] = mapping
        return mapping

    def available_dates(self, since: date, until: date) -> list[date]:
        if since > until:
            return []
        dates: list[date] = []
        for year in range(since.year, until.year + 1):
            idx = self._year_index(year)
            dates.extend(d for d in idx if since <= d <= until)
        return sorted(dates)

    def latest_available_date(
        self, not_after: date | None = None, max_years_back: int = 6
    ) -> date | None:
        not_after = not_after or datetime.now(timezone.utc).date()
        for year in range(not_after.year, not_after.year - max_years_back, -1):
            idx = self._year_index(year)
            candidates = [d for d in idx if d <= not_after]
            if candidates:
                return max(candidates)
        return None

    def fetch(self, target_date: date) -> pl.DataFrame | None:
        fmt = self._year_index(target_date.year).get(target_date)
        if fmt == "zip":
            return self._fetch_zip(target_date)
        if fmt == "zst":
            return self._fetch_zst(target_date)

        # Not found in the indexed listing (e.g. an explicit --year/--month/--day
        # was passed without going through available_dates() first). Try both
        # known layouts before giving up on the day.
        df = self._fetch_zst(target_date)
        if df is not None:
            return df
        return self._fetch_zip(target_date)

    def _fetch_zst(self, target_date: date) -> pl.DataFrame | None:
        date_str = target_date.isoformat()
        url = f"{self.BASE_URL}/{target_date.year}/ais-{date_str}.csv.zst"
        logger.info(f"Streaming AIS data (zst) from {url}...")

        resp = self._get(url, stream=True)
        if resp is None:
            return None

        try:
            dctx = zstd.ZstdDecompressor()
            with dctx.stream_reader(resp.raw) as reader:
                csv_buffer = io.BytesIO(reader.read())
        except zstd.ZstdError as e:
            logger.error(f"Failed to decompress {url}: {e}")
            return None
        finally:
            resp.close()

        try:
            df = pl.read_csv(
                csv_buffer,
                columns=["mmsi", "latitude", "longitude", "base_date_time", "vessel_name", "imo"],
                ignore_errors=True,
            )
        except Exception as e:
            logger.error(f"Failed to parse CSV from {url}: {e}")
            return None

        return df.select(NORMALIZED_COLUMNS)

    def _fetch_zip(self, target_date: date) -> pl.DataFrame | None:
        date_str = target_date.strftime("%Y_%m_%d")
        url = f"{self.BASE_URL}/{target_date.year}/AIS_{date_str}.zip"
        logger.info(f"Streaming AIS data (zip archive) from {url}...")

        resp = self._get(url, stream=True)
        if resp is None:
            return None

        try:
            zip_buffer = io.BytesIO(resp.content)
        finally:
            resp.close()

        try:
            with zipfile.ZipFile(zip_buffer) as zf:
                inner_names = zf.namelist()
                if not inner_names:
                    logger.error(f"Zip archive {url} is empty.")
                    return None
                with zf.open(inner_names[0]) as f:
                    df = pl.read_csv(
                        f,
                        columns=["MMSI", "LAT", "LON", "BaseDateTime", "VesselName", "IMO"],
                        ignore_errors=True,
                    )
        except Exception as e:
            logger.error(f"Failed to parse zip archive {url}: {e}")
            return None

        df = df.rename(
            {
                "MMSI": "mmsi",
                "LAT": "latitude",
                "LON": "longitude",
                "BaseDateTime": "base_date_time",
                "VesselName": "vessel_name",
                "IMO": "imo",
            }
        )
        return df.select(NORMALIZED_COLUMNS)
