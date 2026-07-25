"""Tests for availability-aware date resolution in src/ingest_motherduck.py.

Uses a fake AISSource stub — no live network access.
"""

import argparse
from datetime import date

from src.ingest_motherduck import resolve_dates_to_process


class FakeSource:
    """Stub AISSource with a fixed 'latest' date and a canned available_dates()."""

    def __init__(self, latest: date | None, published: list[date] | None = None):
        self._latest = latest
        self._published = published or []

    def latest_available_date(self, not_after=None):
        return self._latest

    def available_dates(self, since, until):
        return sorted(d for d in self._published if since <= d <= until)

    def fetch(self, target_date):  # pragma: no cover - not exercised here
        raise NotImplementedError


def make_args(year=None, month=None, day=None, backfill_from=None):
    return argparse.Namespace(year=year, month=month, day=day, backfill_from=backfill_from)


def test_default_probes_latest_available_date_not_yesterday():
    """The core bug fix: default must NOT be 'yesterday' — it must be whatever
    the source reports as actually published, even if that's months old."""
    source = FakeSource(latest=date(2025, 12, 31))
    args = make_args()

    dates = resolve_dates_to_process(source, args, today=date(2026, 7, 25))

    assert dates == [date(2025, 12, 31)]


def test_default_returns_empty_when_source_has_nothing():
    source = FakeSource(latest=None)
    args = make_args()

    dates = resolve_dates_to_process(source, args, today=date(2026, 7, 25))

    assert dates == []


def test_backfill_from_returns_published_gap_dates_only():
    published = [date(2025, 12, 29), date(2025, 12, 31)]  # note: 12-30 missing
    source = FakeSource(latest=date(2025, 12, 31), published=published)
    args = make_args(backfill_from="2025-12-01")

    dates = resolve_dates_to_process(source, args, today=date(2026, 7, 25))

    # Only actually-published dates are returned; the gap on 12-30 is silently
    # excluded here (never attempted) rather than crashing anything downstream.
    assert dates == [date(2025, 12, 29), date(2025, 12, 31)]


def test_backfill_from_aborts_cleanly_when_latest_unknown():
    source = FakeSource(latest=None)
    args = make_args(backfill_from="2025-12-01")

    dates = resolve_dates_to_process(source, args, today=date(2026, 7, 25))

    assert dates == []


def test_explicit_year_month_day_is_unaffected_by_availability():
    source = FakeSource(latest=date(2020, 1, 1))  # source thinks nothing recent exists
    args = make_args(year=2024, month=6, day=15)

    dates = resolve_dates_to_process(source, args, today=date(2026, 7, 25))

    assert dates == [date(2024, 6, 15)]


def test_explicit_year_and_month_spans_full_month():
    source = FakeSource(latest=None)
    args = make_args(year=2024, month=2)  # leap year

    dates = resolve_dates_to_process(source, args, today=date(2026, 7, 25))

    assert dates[0] == date(2024, 2, 1)
    assert dates[-1] == date(2024, 2, 29)
    assert len(dates) == 29


def test_explicit_year_only_is_capped_at_today():
    source = FakeSource(latest=None)
    args = make_args(year=2026)

    dates = resolve_dates_to_process(source, args, today=date(2026, 7, 25))

    assert dates[0] == date(2026, 1, 1)
    assert dates[-1] == date(2026, 7, 25)


def test_explicit_future_day_is_excluded():
    source = FakeSource(latest=None)
    args = make_args(year=2026, month=8, day=1)

    dates = resolve_dates_to_process(source, args, today=date(2026, 7, 25))

    assert dates == []
