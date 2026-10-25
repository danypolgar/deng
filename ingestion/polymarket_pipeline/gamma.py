
import logging
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone

import requests

from .http_client import ThrottledSession

log = logging.getLogger(__name__)

PAGE_SIZE = 100  # the API caps limit at 100

# Gamma gives each query ~2 s. Paging newest-first by closedTime it cannot get further back
# than ~38,000 closed markets (all volumes), i.e. roughly 4 weeks. The rest of the window is
# then read in endDate slices: an endDate range is a cheap server-side filter.
END_DATE_SLICE = timedelta(days=7)
# closedTime - endDate in a sample of 4,000 markets: -455 to +224 days, 99% within -440..+21.
END_DATE_BEFORE_GAP = timedelta(days=365)
END_DATE_AFTER_GAP = timedelta(days=730)


def parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    value = value.strip().replace("Z", "+00:00")
    if value.endswith("+00"):
        value += ":00"
    ts = datetime.fromisoformat(value)
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


def _iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def _pages(session: ThrottledSession, base_url: str, params: dict) -> Iterator[list[dict]]:
    cursor = None
    while True:
        page_params = dict(params, after_cursor=cursor) if cursor else params
        body = session.get_json(f"{base_url}/markets/keyset", params=page_params)
        markets = body.get("markets") or []
        yield markets
        cursor = body.get("next_cursor")
        if not markets or not cursor:
            return


def _newest_first(
    session: ThrottledSession, base_url: str, params: dict, start: datetime, end: datetime
) -> Iterator[tuple[dict, datetime]]:
    """(market, closedTime) for start <= closedTime < end, paging by closedTime descending."""
    for markets in _pages(session, base_url, dict(params, order="closedTime", ascending="false")):
        reached_older = False
        for market in markets:
            closed_time = parse_ts(market.get("closedTime"))
            if closed_time is None or closed_time >= end:
                continue
            if closed_time < start:
                reached_older = True
                continue
            yield market, closed_time
        if reached_older:
            return


def _by_end_date(
    session: ThrottledSession, base_url: str, params: dict, start: datetime, end: datetime
) -> Iterator[dict]:
    """Markets with start <= closedTime < end, read in endDate slices."""
    slice_start = start - END_DATE_BEFORE_GAP
    slices = 0
    while slice_start < end + END_DATE_AFTER_GAP:
        slice_end = slice_start + END_DATE_SLICE
        slice_params = dict(params, end_date_min=_iso(slice_start), end_date_max=_iso(slice_end))
        try:
            for market, _ in _newest_first(session, base_url, slice_params, start, end):
                yield market
        except requests.RequestException:
            # Slice full of markets that closed before `start`: sorting it by closedTime times
            # out, so read the whole slice in endDate order (indexed) and filter here.
            log.info("endDate slice %s: reading it in endDate order", slice_start.date())
            for markets in _pages(session, base_url, dict(slice_params, order="endDate", ascending="true")):
                for market in markets:
                    closed_time = parse_ts(market.get("closedTime"))
                    if closed_time is not None and start <= closed_time < end:
                        yield market
        slice_start = slice_end
        slices += 1
    log.info("Gamma endDate fallback finished after %d slice(s)", slices)


def iter_closed_markets(
    session: ThrottledSession,
    base_url: str,
    window_start: datetime,
    window_end: datetime,
    min_volume_usd: float,
    min_days_open: int,
) -> Iterator[dict]:
    params = {
        "closed": "true",
        "limit": PAGE_SIZE,
        "volume_num_min": min_volume_usd,
        "start_date_max": _iso(window_end - timedelta(days=min_days_open)),
        "include_tag": "true",
    }
    seen: set[str] = set()
    covered_from = window_end  # the newest-first pass has every market closed in [covered_from, window_end)
    try:
        for market, closed_time in _newest_first(session, base_url, params, window_start, window_end):
            covered_from = min(covered_from, closed_time)
            seen.add(str(market["id"]))
            yield market
        covered_from = window_start
    except requests.RequestException as exc:
        log.warning("Gamma closedTime paging stopped at %s (%s); reading the rest of the window by endDate",
                    covered_from.isoformat(), exc)
    log.info("Gamma closedTime paging: %d market(s)", len(seen))
    if covered_from <= window_start:
        return

    # covered_from itself is included again: the failed page may have held more markets with that closedTime.
    for market in _by_end_date(session, base_url, params, window_start, covered_from + timedelta(seconds=1)):
        market_id = str(market["id"])
        if market_id not in seen:
            seen.add(market_id)
            yield market
    log.info("Gamma total: %d market(s)", len(seen))
