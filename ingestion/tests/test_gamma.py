from datetime import datetime, timedelta, timezone

import pytest
import requests

from polymarket_pipeline import gamma

UTC = timezone.utc
END = datetime(2026, 10, 25, tzinfo=UTC)
START = END - timedelta(days=30)
CUTOFF = END - timedelta(days=28)  # the fake API cannot page by closedTime past this


def ts(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S+00")


def mk(mid, closed, end_offset=timedelta(0)):
    return {"id": mid, "closedTime": ts(closed), "endDate": (closed + end_offset).isoformat()}


class FakeGamma:
    """Keyset API over `markets` that times out like Gamma: closedTime paging past CUTOFF
    fails, and so does closedTime paging of an endDate slice listed in `slow_slices`."""

    def __init__(self, markets, slow_slices=()):
        self.markets = markets
        self.slow_slices = set(slow_slices)
        self.end_date_order_reads = 0

    def get_json(self, url, params=None):
        rows = self.markets
        if "end_date_min" in params:
            lo, hi = gamma.parse_ts(params["end_date_min"]), gamma.parse_ts(params["end_date_max"])
            rows = [m for m in rows if m["endDate"] and lo <= gamma.parse_ts(m["endDate"]) <= hi]
        if params["order"] == "endDate":
            self.end_date_order_reads += 1
            rows = sorted(rows, key=lambda m: m["endDate"])
        else:
            rows = sorted(rows, key=lambda m: m["closedTime"], reverse=True)
        offset = int(params.get("after_cursor") or 0)
        page = rows[offset:offset + params["limit"]]
        if params["order"] == "closedTime":
            slow = "end_date_min" in params and params["end_date_min"][:10] in self.slow_slices
            if slow or (page and gamma.parse_ts(page[-1]["closedTime"]) < CUTOFF and "end_date_min" not in params):
                raise requests.exceptions.RetryError("too many 500 error responses")
        nxt = offset + len(page)
        return {"markets": page, "next_cursor": str(nxt) if nxt < len(rows) else None}


def run(api):
    return [m["id"] for m in gamma.iter_closed_markets(api, "https://x", START, END, 10_000, 30)]


def test_shallow_window_uses_closed_time_paging_only():
    api = FakeGamma([mk(f"m{i}", END - timedelta(hours=i)) for i in range(1, 251)])
    assert run(api) == [f"m{i}" for i in range(1, 251)]
    assert api.end_date_order_reads == 0


def test_deep_window_is_completed_by_end_date_slices_without_duplicates():
    markets = [mk(f"m{i}", END - timedelta(hours=3 * i)) for i in range(1, 240)]  # down to START + 3h
    markets += [
        mk("early_close", START + timedelta(hours=5), end_offset=timedelta(days=300)),
        mk("late_close", START + timedelta(hours=7), end_offset=-timedelta(days=200)),
        mk("too_old", START - timedelta(hours=1)),
        mk("too_new", END + timedelta(hours=1)),
    ]
    ids = run(FakeGamma(markets))
    expected = {m["id"] for m in markets} - {"too_old", "too_new"}
    assert set(ids) == expected
    assert len(ids) == len(expected)


def test_slow_end_date_slice_is_read_in_end_date_order():
    markets = [mk(f"m{i}", END - timedelta(hours=3 * i)) for i in range(1, 240)]
    late = mk("late_close", START + timedelta(hours=7), end_offset=-timedelta(days=20))
    markets.append(late)
    first = gamma.parse_ts(late["endDate"])
    # the 7-day slice containing late_close's endDate times out when sorted by closedTime
    slice_start = START - gamma.END_DATE_BEFORE_GAP
    while slice_start + gamma.END_DATE_SLICE <= first:
        slice_start += gamma.END_DATE_SLICE
    api = FakeGamma(markets, slow_slices={slice_start.strftime("%Y-%m-%d")})
    assert "late_close" in run(api)
    assert api.end_date_order_reads >= 1


def test_end_date_order_failure_is_raised():
    class Broken(FakeGamma):
        def get_json(self, url, params=None):
            if params["order"] == "endDate":
                raise requests.exceptions.RetryError("too many 500 error responses")
            return super().get_json(url, params)

    markets = [mk(f"m{i}", END - timedelta(hours=3 * i)) for i in range(1, 240)]
    slices = set()
    s = START - gamma.END_DATE_BEFORE_GAP
    while s < END + gamma.END_DATE_AFTER_GAP:
        slices.add(s.strftime("%Y-%m-%d"))
        s += gamma.END_DATE_SLICE
    with pytest.raises(requests.RequestException):
        run(Broken(markets, slow_slices=slices))
