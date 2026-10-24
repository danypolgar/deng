"""Integration tests for the SQL layer (staging views, snapshot, summary, DQ checks).

Every test session creates its own throw-away database, applies the real SQL files and
loads small hand-made fixtures whose correct results are known. The tests need a
PostgreSQL server. They are skipped if none is reachable.

Connection (first match wins):
  1. TEST_PG_ADMIN_DSN, e.g. "host=localhost port=5433 user=polymarket password=... dbname=postgres"
  2. PG_HOST / PG_PORT / PG_USER / PG_PASSWORD (set inside `docker compose run pipeline`)
"""

import dataclasses
import os
import uuid
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from polymarket_pipeline import db
from polymarket_pipeline.config import Settings

UTC = timezone.utc
CLOSE = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)  # common close time of the fixtures
START = CLOSE - timedelta(days=60)                 # open for 60 days -> every horizon exists


def _admin_dsn() -> str | None:
    if os.environ.get("TEST_PG_ADMIN_DSN"):
        return os.environ["TEST_PG_ADMIN_DSN"]
    if os.environ.get("PG_PASSWORD"):
        return (
            f"host={os.environ.get('PG_HOST', 'localhost')} port={os.environ.get('PG_PORT', '5432')} "
            f"user={os.environ.get('PG_USER', 'polymarket')} password={os.environ['PG_PASSWORD']} dbname=postgres"
        )
    return None


@pytest.fixture(scope="module")
def settings():
    admin = _admin_dsn()
    if not admin:
        pytest.skip("no PostgreSQL configured (set TEST_PG_ADMIN_DSN)")
    try:
        admin_conn = psycopg.connect(admin, autocommit=True, connect_timeout=3)
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")

    name = f"deng_test_{uuid.uuid4().hex[:8]}"
    admin_conn.execute(f'create database "{name}"')
    params = psycopg.conninfo.conninfo_to_dict(admin)
    base = Settings()
    s = dataclasses.replace(
        base,
        pg_host=params.get("host", "localhost"),
        pg_port=int(params.get("port", 5432)),
        pg_user=params["user"],
        pg_password=params.get("password", ""),
        pg_database=name,
        run_id="test",
    )
    try:
        yield s
    finally:
        admin_conn.execute(f'drop database if exists "{name}" with (force)')
        admin_conn.close()


def market(mid, *, tags=("politics",), volume=20_000, start=START, close=CLOSE,
           outcomes='["Yes", "No"]', prices='["1", "0"]', status="resolved"):
    return {
        "id": mid,
        "question": f"Fixture market {mid}?",
        "startDate": start.isoformat(),
        "endDate": close.isoformat(),
        "closedTime": close.strftime("%Y-%m-%d %H:%M:%S+00"),
        "outcomes": outcomes,
        "outcomePrices": prices,
        "clobTokenIds": f'["yes-{mid}", "no-{mid}"]',
        "volumeNum": volume,
        "umaResolutionStatus": status,
        "tags": [{"label": t} for t in tags],
        "events": [{"id": f"ev-{mid}", "title": "Fixture event"}],
    }


def history(*points):
    """points: (datetime, price) pairs."""
    return {"history": [{"t": int(ts.timestamp()), "p": p} for ts, p in points]}


def h(days, delta=timedelta(0)):
    """Timestamp of a horizon (days before close) plus an offset."""
    return CLOSE - timedelta(days=days) + delta


MARKETS = [
    # In scope. 30d: last price before the horizon is 0.35 (bucket 0.3). A price exactly AT
    # the horizon (0.99) must be ignored (no look-ahead). 7d: 1.0 -> bucket 0.9 (top bucket).
    market("ok_politics"),
    # In scope, crypto wins over politics (priority 1 < 3). Only stale price at 30d.
    market("ok_crypto", tags=("politics", "bitcoin")),
    market("ex_sports", tags=("sports", "politics")),
    market("ex_other", tags=("ai",)),
    market("ex_low_volume", volume=9_999),
    market("ex_short", start=CLOSE - timedelta(days=29)),
    market("ex_unclean", prices='["0.5", "0.5"]'),
    market("ex_not_binary", outcomes='["Trump", "Harris"]'),
    market("ex_unresolved", status="disputed"),
]

HISTORIES = {
    "ok_politics": history(
        (h(30, -timedelta(days=1)), 0.20),
        (h(30, -timedelta(hours=1)), 0.35),
        (h(30), 0.99),                      # exactly at horizon -> must not be used
        (h(7, -timedelta(hours=2)), 1.0),
        (h(1, -timedelta(hours=3)), 0.97),
    ),
    "ok_crypto": history(
        (h(30, -timedelta(days=4)), 0.50),  # > 3 days before the 30d horizon -> stale
        (h(7, -timedelta(hours=5)), 0.05),
        (h(1, -timedelta(hours=5)), 0.02),
    ),
}


@pytest.fixture(scope="module")
def conn(settings):
    with db.connect(settings) as c:
        db.ensure_schema(c, settings)
        db.upsert_markets(c, MARKETS, "test")
        for mid, payload in HISTORIES.items():
            db.save_price_history(c, mid, f"yes-{mid}", payload, "test")
        db.run_sql_files(c, settings.sql_dir, db.TRANSFORM_FILES)
        yield c


def q(conn, sql, *params):
    return conn.execute(sql, params).fetchall()


# --- staging ---------------------------------------------------------------------------

def test_exclusion_reasons(conn):
    got = dict(q(conn, "select market_id, exclusion_reason from staging.stg_markets"))
    assert got == {
        "ok_politics": None,
        "ok_crypto": None,
        "ex_sports": "sports",
        "ex_other": "out_of_scope_category",
        "ex_low_volume": "low_volume",
        "ex_short": "open_less_than_30_days",
        "ex_unclean": "unclean_resolution",
        "ex_not_binary": "not_binary_yes_no",
        "ex_unresolved": "not_resolved",
    }


def test_category_priority_and_outcome(conn):
    got = dict(q(conn, "select market_id, category from staging.stg_markets where in_scope"))
    assert got == {"ok_politics": "politics", "ok_crypto": "crypto"}
    assert q(conn, "select outcome, yes_token_id from staging.stg_markets where market_id = 'ok_politics'") == [
        (1, "yes-ok_politics")
    ]


# --- fct_market_snapshot ---------------------------------------------------------------

def snapshot(conn):
    rows = q(conn, "select market_id, horizon_days, implied_prob, prob_bucket from marts.fct_market_snapshot")
    return {(m, hd): (float(p), float(b)) for m, hd, p, b in rows}


def test_snapshot_uses_last_price_strictly_before_horizon(conn):
    snap = snapshot(conn)
    assert snap[("ok_politics", 30)] == (0.35, 0.3)  # not 0.99 at the horizon itself


def test_snapshot_top_bucket_is_0_9(conn):
    assert snapshot(conn)[("ok_politics", 7)] == (1.0, 0.9)


def test_snapshot_drops_stale_prices(conn):
    snap = snapshot(conn)
    assert ("ok_crypto", 30) not in snap          # only price is 4 days old
    assert snap[("ok_crypto", 7)] == (0.05, 0.0)


def test_snapshot_only_in_scope_markets(conn):
    assert {m for m, _ in snapshot(conn)} == {"ok_politics", "ok_crypto"}
    assert len(snapshot(conn)) == 5


def test_squared_error(conn):
    (err,) = q(conn, "select squared_error from marts.fct_market_snapshot "
                     "where market_id = 'ok_crypto' and horizon_days = 1")[0]
    assert float(err) == pytest.approx((0.02 - 1) ** 2)


def test_transform_is_idempotent(conn, settings):
    before = q(conn, "select * from marts.fct_market_snapshot order by 1, 2")
    db.run_sql_files(conn, settings.sql_dir, db.TRANSFORM_FILES)
    after = q(conn, "select * from marts.fct_market_snapshot order by 1, 2")
    strip = lambda rows: [r[:-1] for r in rows]   # built_at changes
    assert strip(before) == strip(after)


# --- summary and DQ -------------------------------------------------------------------

def test_calibration_summary_pools_all_categories(conn):
    # 1-day prices: politics 0.97 (bucket 0.9), crypto 0.02 (bucket 0.0); both resolved Yes.
    rows = q(conn, "select category, prob_bucket, market_count, observed_frequency "
                   "from marts.mart_calibration_summary where horizon_days = 1 order by category, prob_bucket")
    got = [(c, float(b), n, float(f)) for c, b, n, f in rows]
    assert got == [
        ("all", 0.0, 1, 1.0), ("all", 0.9, 1, 1.0),
        ("crypto", 0.0, 1, 1.0),
        ("politics", 0.9, 1, 1.0),
    ]


def test_dq_error_checks_pass(conn):
    failing = q(conn, "select check_name from marts.dq_results where severity = 'error' and failing_rows > 0")
    assert failing == []


# --- single source of truth for scope thresholds -------------------------------------

def test_scope_thresholds_come_from_settings(conn, settings):
    stricter = dataclasses.replace(settings, min_volume_usd=50_000)
    try:
        db.write_scope_settings(conn, stricter)
        got = dict(q(conn, "select market_id, exclusion_reason from staging.stg_markets "
                           "where market_id in ('ok_politics', 'ok_crypto')"))
        assert got == {"ok_politics": "low_volume", "ok_crypto": "low_volume"}
    finally:
        db.write_scope_settings(conn, settings)
    assert q(conn, "select count(*) from staging.stg_markets where in_scope") == [(2,)]


# --- retry of empty price histories ---------------------------------------------------

def test_empty_history_is_retried_until_limit(conn):
    mid = "ok_empty"
    db.upsert_markets(conn, [market(mid)], "test")
    db.save_price_history(conn, mid, f"yes-{mid}", {"history": []}, "test")
    try:
        for attempt in range(1, 4):
            todo = [m for m, _ in db.markets_needing_prices(conn, None, None, max_empty_attempts=3)]
            if attempt < 3:
                assert mid in todo, f"attempt {attempt}: empty history must be retried"
                db.save_price_history(conn, mid, f"yes-{mid}", {"history": []}, "test")
            else:
                assert mid not in todo, "after 3 attempts the market must be left alone"
        assert q(conn, "select fetch_attempts from raw.price_history where market_id = %s", mid) == [(3,)]
    finally:
        conn.execute("delete from raw.price_history where market_id = %s", (mid,))
        conn.execute("delete from raw.markets where market_id = %s", (mid,))


def test_market_without_history_is_fetched(conn):
    mid = "ok_new"
    db.upsert_markets(conn, [market(mid)], "test")
    try:
        assert mid in [m for m, _ in db.markets_needing_prices(conn, None, None)]
    finally:
        conn.execute("delete from raw.markets where market_id = %s", (mid,))
