
import logging
from datetime import datetime
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

from .config import Settings

log = logging.getLogger(__name__)


SCHEMA_FILES = ("00_raw.sql", "10_staging.sql")
TRANSFORM_FILES = ("20_fct_market_snapshot.sql", "30_mart_calibration_summary.sql", "40_dq_checks.sql")


def connect(settings: Settings) -> psycopg.Connection:
    return psycopg.connect(settings.pg_dsn, autocommit=True)


def run_sql_files(conn: psycopg.Connection, sql_dir: Path, files: tuple[str, ...]) -> None:
    with conn.transaction():
        for name in files:
            log.info("Applying %s", name)
            conn.execute((sql_dir / name).read_text())


def ensure_schema(conn: psycopg.Connection, settings: Settings) -> None:
    run_sql_files(conn, settings.sql_dir, SCHEMA_FILES)


def upsert_markets(conn: psycopg.Connection, markets: list[dict], run_id: str) -> int:
    rows = [(str(m["id"]), m.get("closedTime"), Jsonb(m), run_id) for m in markets]
    with conn.transaction(), conn.cursor() as cur:
        cur.executemany(
            """
            insert into raw.markets (market_id, closed_time, payload, run_id, ingested_at)
            values (%s, %s::timestamptz, %s, %s, now())
            on conflict (market_id) do update
               set closed_time = excluded.closed_time,
                   payload     = excluded.payload,
                   run_id      = excluded.run_id,
                   ingested_at = excluded.ingested_at
            """,
            rows,
        )
    return len(rows)


def markets_needing_prices(
    conn: psycopg.Connection, window_start: datetime | None, window_end: datetime | None
) -> list[tuple[str, str]]:
    if window_start is None:
        query = """
            select s.market_id, s.yes_token_id
              from staging.stg_markets s
             where s.in_scope
               and not exists (select 1 from raw.price_history p where p.market_id = s.market_id)
             order by s.closed_time desc
        """
        params = ()
    else:
        query = """
            select s.market_id, s.yes_token_id
              from staging.stg_markets s
             where s.in_scope and s.closed_time >= %s and s.closed_time < %s
             order by s.closed_time desc
        """
        params = (window_start, window_end)
    return conn.execute(query, params).fetchall()


def save_price_history(conn: psycopg.Connection, market_id: str, token_id: str, payload: dict, run_id: str) -> None:
    points = len(payload.get("history") or [])
    with conn.transaction():
        conn.execute(
            """
            insert into raw.price_history (market_id, token_id, payload, point_count, run_id, ingested_at)
            values (%s, %s, %s, %s, %s, now())
            on conflict (market_id) do update
               set token_id    = excluded.token_id,
                   payload     = excluded.payload,
                   point_count = excluded.point_count,
                   run_id      = excluded.run_id,
                   ingested_at = excluded.ingested_at
            """,
            (market_id, token_id, Jsonb(payload), points, run_id),
        )


def markets_watermark(conn: psycopg.Connection) -> datetime | None:
    row = conn.execute(
        "select max(window_end) from raw.ingestion_log where step = 'markets' and status = 'success'"
    ).fetchone()
    return row[0]


def log_run(
    conn: psycopg.Connection,
    *,
    run_id: str,
    step: str,
    status: str,
    started_at: datetime,
    window_start: datetime | None = None,
    window_end: datetime | None = None,
    fetched: int = 0,
    loaded: int = 0,
    failed: int = 0,
    message: str | None = None,
) -> None:
    with conn.transaction():
        conn.execute(
            """
            insert into raw.ingestion_log
                (run_id, step, status, window_start, window_end,
                 records_fetched, records_loaded, records_failed, message, started_at, finished_at)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            """,
            (run_id, step, status, window_start, window_end, fetched, loaded, failed, message, started_at),
        )
