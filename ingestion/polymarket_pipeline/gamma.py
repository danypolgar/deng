
import logging
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone

from .http_client import ThrottledSession

log = logging.getLogger(__name__)

PAGE_SIZE = 100


def parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    value = value.strip().replace("Z", "+00:00")
    if value.endswith("+00"):
        value += ":00"
    ts = datetime.fromisoformat(value)
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


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
        "order": "closedTime",
        "ascending": "false",
        "volume_num_min": min_volume_usd,
        "start_date_max": (window_end - timedelta(days=min_days_open)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "include_tag": "true",
    }
    cursor = None
    pages = 0
    while True:
        page_params = dict(params, after_cursor=cursor) if cursor else params
        body = session.get_json(f"{base_url}/markets/keyset", params=page_params)
        markets = body.get("markets") or []
        pages += 1

        reached_older = False
        for market in markets:
            closed_time = parse_ts(market.get("closedTime"))
            if closed_time is None:
                continue
            if closed_time >= window_end:
                continue
            if closed_time < window_start:
                reached_older = True
                continue
            yield market

        cursor = body.get("next_cursor")
        if reached_older or not markets or not cursor:
            log.info("Gamma paging finished after %d page(s)", pages)
            return
