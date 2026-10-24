from datetime import datetime, timedelta, timezone

import pytest

from polymarket_pipeline.cli import _parse_cli_ts, resolve_window
from polymarket_pipeline.config import Settings
from polymarket_pipeline.gamma import parse_ts

UTC = timezone.utc


@pytest.mark.parametrize(
    "value, expected",
    [
        ("2026-10-01T12:00:00Z", datetime(2026, 10, 1, 12, tzinfo=UTC)),
        ("2026-10-01 12:00:00+00", datetime(2026, 10, 1, 12, tzinfo=UTC)),
        ("2026-10-01T12:00:00", datetime(2026, 10, 1, 12, tzinfo=UTC)),
        ("", None),
        (None, None),
    ],
)
def test_parse_ts(value, expected):
    assert parse_ts(value) == expected


def test_parse_cli_ts_date_only_is_midnight_utc():
    assert _parse_cli_ts("2026-10-01") == datetime(2026, 10, 1, tzinfo=UTC)
    assert _parse_cli_ts("  ") is None


def test_window_first_run_backfills_lookback():
    s = Settings()
    start, end = resolve_window(None, None, None, s)
    assert timedelta(days=s.lookback_days) - (end - start) < timedelta(seconds=5)


def test_window_incremental_uses_watermark_minus_overlap():
    s = Settings()
    wm = datetime.now(UTC) - timedelta(days=1)
    start, _ = resolve_window(None, None, wm, s)
    assert start == wm - timedelta(minutes=s.overlap_minutes)


def test_window_watermark_capped_at_lookback():
    s = Settings()
    start, end = resolve_window(None, None, datetime(2000, 1, 1, tzinfo=UTC), s)
    assert end - start <= timedelta(days=s.lookback_days, seconds=5)


def test_window_end_never_in_future_and_invalid_window_rejected():
    s = Settings()
    _, end = resolve_window(None, datetime(2999, 1, 1, tzinfo=UTC), None, s)
    assert end <= datetime.now(UTC)
    with pytest.raises(SystemExit):
        resolve_window(datetime(2026, 10, 2, tzinfo=UTC), datetime(2026, 10, 1, tzinfo=UTC), None, s)
