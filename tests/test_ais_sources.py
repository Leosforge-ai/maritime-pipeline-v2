"""Tests for the AIS source abstraction (src/ais_sources.py).

All HTTP/websocket access is mocked — no live network access, and no live
AISSTREAM_API_KEY, is used or required.
"""

import asyncio
import io
import json
import time
import zipfile
from datetime import date, datetime, timedelta, timezone

import polars as pl
import pytest
import zstandard as zstd

from src.ais_sources import NORMALIZED_COLUMNS, AisstreamSource, NoaaMarineCadastreSource

# Fixture HTML fragments mimicking NOAA's real directory-listing pages, one for a
# "current" year published as daily .csv.zst, one for an "archived" year
# repackaged into daily .zip bundles, based on a live probe on 2026-07-25.
ZST_YEAR_INDEX_HTML = """
<h1>AIS Data for 2025</h1>
<a href="ais-2025-12-30.csv.zst">ais-2025-12-30.csv.zst</a><br>
<a href="ais-2025-12-31.csv.zst">ais-2025-12-31.csv.zst</a><br>
<a href="index.html">index.html</a><br>
"""

ZIP_YEAR_INDEX_HTML = """
<h1>AIS Data for 2024</h1>
<a href="AIS_2024_01_01.zip">AIS_2024_01_01.zip</a><br>
<a href="AIS_2024_01_02.zip">AIS_2024_01_02.zip</a><br>
<a href="index.html">index.html</a><br>
"""


class FakeResponse:
    def __init__(self, status_code=200, text="", content=b""):
        self.status_code = status_code
        self.text = text
        self.content = content
        self.raw = io.BytesIO(content)

    def close(self):
        pass


class FakeSession:
    """Maps exact URLs to canned FakeResponses (or None for a 404/never-existed year)."""

    def __init__(self, routes: dict[str, FakeResponse]):
        self._routes = routes
        self.calls: list[str] = []

    def get(self, url, stream=False, timeout=None):
        self.calls.append(url)
        if url in self._routes:
            return self._routes[url]
        return FakeResponse(status_code=404)


def zst_bytes(csv_text: str) -> bytes:
    return zstd.ZstdCompressor().compress(csv_text.encode())


def zip_bytes(inner_name: str, csv_text: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(inner_name, csv_text)
    return buf.getvalue()


def test_year_index_parses_both_zst_and_zip_patterns():
    session = FakeSession(
        {
            "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2025/": FakeResponse(
                text=ZST_YEAR_INDEX_HTML
            ),
            "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2024/": FakeResponse(
                text=ZIP_YEAR_INDEX_HTML
            ),
        }
    )
    source = NoaaMarineCadastreSource(session=session)

    idx_2025 = source._year_index(2025)
    assert idx_2025 == {
        date(2025, 12, 30): "zst",
        date(2025, 12, 31): "zst",
    }

    idx_2024 = source._year_index(2024)
    assert idx_2024 == {
        date(2024, 1, 1): "zip",
        date(2024, 1, 2): "zip",
    }


def test_year_index_is_cached_per_instance():
    session = FakeSession(
        {
            "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2025/": FakeResponse(
                text=ZST_YEAR_INDEX_HTML
            )
        }
    )
    source = NoaaMarineCadastreSource(session=session)
    source._year_index(2025)
    source._year_index(2025)
    assert session.calls.count("https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2025/") == 1


def test_year_index_404_returns_empty_mapping():
    session = FakeSession({})  # nothing published for 2026 yet
    source = NoaaMarineCadastreSource(session=session)
    assert source._year_index(2026) == {}


def test_available_dates_filters_to_range_across_years():
    session = FakeSession(
        {
            "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2025/": FakeResponse(
                text=ZST_YEAR_INDEX_HTML
            ),
            "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2024/": FakeResponse(
                text=ZIP_YEAR_INDEX_HTML
            ),
        }
    )
    source = NoaaMarineCadastreSource(session=session)

    dates = source.available_dates(since=date(2024, 1, 2), until=date(2025, 12, 30))
    assert dates == [date(2024, 1, 2), date(2025, 12, 30)]


def test_latest_available_date_walks_back_when_current_year_unpublished():
    session = FakeSession(
        {
            # 2026 (the "not_after" year) has nothing published yet.
            "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2025/": FakeResponse(
                text=ZST_YEAR_INDEX_HTML
            ),
        }
    )
    source = NoaaMarineCadastreSource(session=session)

    latest = source.latest_available_date(not_after=date(2026, 7, 25))
    assert latest == date(2025, 12, 31)


def test_latest_available_date_none_when_nothing_found():
    session = FakeSession({})
    source = NoaaMarineCadastreSource(session=session)
    assert source.latest_available_date(not_after=date(2026, 7, 25), max_years_back=1) is None


def test_fetch_zst_normalizes_columns():
    csv_text = (
        "mmsi,base_date_time,longitude,latitude,sog,cog,heading,vessel_name,imo,"
        "call_sign,vessel_type,status,length,width,draft,cargo,transceiver\n"
        "368382440,2025-12-31 00:00:00,-76.43,36.96,4.7,321.6,327,PATRICIA B. MORAN,"
        "IMO1079618,WDP7519,52,0,26,11,5.0,52,A\n"
    )
    url = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2025/ais-2025-12-31.csv.zst"
    session = FakeSession({url: FakeResponse(content=zst_bytes(csv_text))})
    source = NoaaMarineCadastreSource(session=session)
    # Pre-seed the year index so fetch() knows this date is zst-formatted.
    source._year_index_cache[2025] = {date(2025, 12, 31): "zst"}

    df = source.fetch(date(2025, 12, 31))

    assert df is not None
    assert list(df.columns) == NORMALIZED_COLUMNS
    assert df.height == 1
    assert df["mmsi"][0] == 368382440
    assert df["vessel_name"][0] == "PATRICIA B. MORAN"


def test_fetch_zip_normalizes_columns():
    csv_text = (
        "MMSI,BaseDateTime,LAT,LON,SOG,COG,Heading,VesselName,IMO,CallSign,"
        "VesselType,Status,Length,Width,Draft,Cargo,TransceiverClass\n"
        "338075892,2024-01-01T00:00:03,43.65,-70.25,0.0,358.8,511.0,"
        "PILOT BOAT SPRING PT,,WDB8945,90,0,0,0,0.0,90,A\n"
    )
    url = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2024/AIS_2024_01_01.zip"
    session = FakeSession({url: FakeResponse(content=zip_bytes("AIS_2024_01_01.csv", csv_text))})
    source = NoaaMarineCadastreSource(session=session)
    source._year_index_cache[2024] = {date(2024, 1, 1): "zip"}

    df = source.fetch(date(2024, 1, 1))

    assert df is not None
    assert list(df.columns) == NORMALIZED_COLUMNS
    assert df.height == 1
    assert df["mmsi"][0] == 338075892
    assert df["vessel_name"][0] == "PILOT BOAT SPRING PT"
    assert df["imo"][0] is None


def test_fetch_returns_none_when_neither_format_available():
    session = FakeSession({})  # both zst and zip 404
    source = NoaaMarineCadastreSource(session=session)
    assert source.fetch(date(2026, 1, 1)) is None


def test_fetch_falls_back_to_zip_when_zst_404_and_index_unknown():
    """If a date wasn't discovered via available_dates() (e.g. explicit CLI args),
    fetch() should still try both known layouts before giving up."""
    csv_text = (
        "MMSI,BaseDateTime,LAT,LON,SOG,COG,Heading,VesselName,IMO,CallSign,"
        "VesselType,Status,Length,Width,Draft,Cargo,TransceiverClass\n"
        "338075892,2024-01-01T00:00:03,43.65,-70.25,0.0,358.8,511.0,SHIP,,WDB8945,"
        "90,0,0,0,0.0,90,A\n"
    )
    zip_url = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2024/AIS_2024_01_01.zip"
    session = FakeSession({zip_url: FakeResponse(content=zip_bytes("x.csv", csv_text))})
    source = NoaaMarineCadastreSource(session=session)
    # No year-index cache seeded — simulates skipping available_dates().

    df = source.fetch(date(2024, 1, 1))

    assert df is not None
    assert df.height == 1


def test_normalized_dataframe_is_valid_polars_dataframe():
    # Sanity check that the fixture-style DataFrame consumers expect lines up.
    df = pl.DataFrame(
        {
            "mmsi": [1],
            "imo": ["1"],
            "vessel_name": ["x"],
            "latitude": [1.0],
            "longitude": [1.0],
            "base_date_time": ["2025-01-01 00:00:00"],
        }
    )
    assert list(df.columns) == NORMALIZED_COLUMNS


# --- AisstreamSource ---------------------------------------------------------
#
# aisstream.io is a live websocket feed (wss://stream.aisstream.io/v0/stream), not
# a per-day archive, so tests mock `websockets.connect` rather than HTTP. No live
# AISSTREAM_API_KEY is used or required.


def _position_report(mmsi=368382440, lat=36.96, lon=-76.43, name="PATRICIA B. MORAN"):
    return json.dumps(
        {
            "MessageType": "PositionReport",
            "MetaData": {
                "MMSI": mmsi,
                "ShipName": name,
                "time_utc": "2026-07-25 12:00:00 +0000 UTC",
            },
            "Message": {"PositionReport": {"Latitude": lat, "Longitude": lon}},
        }
    )


class FakeWebsocket:
    """Mimics the async context-manager `websockets.connect(...)` returns.

    `recv()` yields queued messages then, once exhausted, sleeps far longer than
    any test's collect window so the deadline (not exhaustion) is what stops the
    collector — matching real aisstream.io behavior of an idle-but-open socket.
    """

    def __init__(self, messages):
        self._messages = list(messages)
        self.sent: list[str] = []
        self.closed = False

    async def send(self, msg):
        self.sent.append(msg)

    async def recv(self):
        if self._messages:
            return self._messages.pop(0)
        await asyncio.sleep(3600)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.closed = True
        return False


def fake_connect(ws: FakeWebsocket):
    def _connect(url, open_timeout=None):
        return ws

    return _connect


def flaky_connect(exc: Exception, ws: FakeWebsocket):
    """First call raises `exc` (simulating a dropped connection); every
    subsequent call returns `ws` (simulating a successful reconnect)."""
    calls = {"n": 0}

    def _connect(url, open_timeout=None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise exc
        return ws

    return _connect


def test_missing_api_key_raises_helpful_error(monkeypatch):
    monkeypatch.delenv("AISSTREAM_API_KEY", raising=False)
    with pytest.raises(ValueError, match="AISSTREAM_API_KEY"):
        AisstreamSource()


def test_missing_api_key_error_mentions_registration():
    with pytest.raises(ValueError, match="aisstream.io"):
        AisstreamSource(api_key="")


def test_fetch_normalizes_position_report(monkeypatch):
    ws = FakeWebsocket([_position_report()])
    monkeypatch.setattr("src.ais_sources.websockets.connect", fake_connect(ws))

    source = AisstreamSource(api_key="test-key", collect_seconds=1)
    df = source.fetch(date(2026, 7, 25))

    assert df is not None
    assert list(df.columns) == NORMALIZED_COLUMNS
    assert df.height == 1
    assert df["mmsi"][0] == 368382440
    assert df["vessel_name"][0] == "PATRICIA B. MORAN"
    assert df["imo"][0] is None
    assert df["latitude"][0] == 36.96
    # Go time.Time's " +0000 UTC" suffix must be stripped so downstream
    # `str.to_datetime(strict=False)` (shared with NOAA) can parse it.
    assert df["base_date_time"][0] == "2026-07-25 12:00:00"


@pytest.mark.parametrize(
    "time_utc,expected",
    [
        ("2026-07-25 12:00:00 +0000 UTC", "2026-07-25 12:00:00"),
        ("2026-07-25 12:00:00.318996 +0000 UTC", "2026-07-25 12:00:00.318996"),
        ("2026-07-25 12:00:00 -0500 UTC", "2026-07-25 12:00:00"),
        ("2026-07-25 12:00:00", "2026-07-25 12:00:00"),  # already bare
        (None, None),
        ("", None),
    ],
)
def test_normalize_time_utc_strips_go_timezone_suffix(time_utc, expected):
    assert AisstreamSource._normalize_time_utc(time_utc) == expected


def test_fetch_subscribes_once_within_rate_limit(monkeypatch):
    ws = FakeWebsocket([_position_report()])
    monkeypatch.setattr("src.ais_sources.websockets.connect", fake_connect(ws))

    source = AisstreamSource(api_key="test-key", collect_seconds=1)
    source.fetch(date(2026, 7, 25))

    assert len(ws.sent) == 1
    subscribed = json.loads(ws.sent[0])
    assert subscribed["APIKey"] == "test-key"
    assert subscribed["FilterMessageTypes"] == ["PositionReport"]


def test_fetch_skips_malformed_messages_without_crashing(monkeypatch):
    ws = FakeWebsocket(
        [
            "not json at all",
            json.dumps({"MessageType": "ShipStaticData"}),  # wrong type, ignored
            json.dumps({"MessageType": "PositionReport"}),  # missing MetaData/Message
            _position_report(mmsi=111222333, name="GHOST SHIP"),
        ]
    )
    monkeypatch.setattr("src.ais_sources.websockets.connect", fake_connect(ws))

    source = AisstreamSource(api_key="test-key", collect_seconds=1)
    df = source.fetch(date(2026, 7, 25))

    assert df is not None
    assert df.height == 1
    assert df["mmsi"][0] == 111222333


def test_fetch_returns_none_when_nothing_collected(monkeypatch):
    ws = FakeWebsocket([])  # recv() will just sleep past the deadline
    monkeypatch.setattr("src.ais_sources.websockets.connect", fake_connect(ws))

    source = AisstreamSource(api_key="test-key", collect_seconds=0.2)
    df = source.fetch(date(2026, 7, 25))

    assert df is None


def test_fetch_never_exceeds_deadline(monkeypatch):
    ws = FakeWebsocket([])  # idle socket; only the deadline should stop us
    monkeypatch.setattr("src.ais_sources.websockets.connect", fake_connect(ws))

    source = AisstreamSource(api_key="test-key", collect_seconds=0.5)
    start = time.monotonic()
    source.fetch(date(2026, 7, 25))
    elapsed = time.monotonic() - start

    assert elapsed < 2.0  # generous bound; real deadline is 0.5s


def test_fetch_reconnects_after_a_dropped_connection(monkeypatch):
    ws = FakeWebsocket([_position_report()])
    monkeypatch.setattr(
        "src.ais_sources.websockets.connect",
        flaky_connect(OSError("connection reset"), ws),
    )

    source = AisstreamSource(api_key="test-key", collect_seconds=2)
    df = source.fetch(date(2026, 7, 25))

    assert df is not None
    assert df.height == 1


def test_fetch_reconnects_after_a_stalled_handshake(monkeypatch):
    """websockets.connect(..., open_timeout=...) raises builtin TimeoutError on a
    stalled handshake — this must trigger a reconnect, not kill the whole window."""
    ws = FakeWebsocket([_position_report()])
    monkeypatch.setattr(
        "src.ais_sources.websockets.connect",
        flaky_connect(TimeoutError("timed out during handshake"), ws),
    )

    source = AisstreamSource(api_key="test-key", collect_seconds=2)
    df = source.fetch(date(2026, 7, 25))

    assert df is not None
    assert df.height == 1


def test_available_dates_and_latest_are_always_today():
    source = AisstreamSource(api_key="test-key")
    today = datetime.now(timezone.utc).date()

    assert source.available_dates(today - timedelta(days=1), today) == [today]
    assert source.available_dates(today + timedelta(days=1), today + timedelta(days=2)) == []
    assert source.latest_available_date() == today
