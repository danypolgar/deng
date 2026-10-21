import argparse
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from . import clob, db, gamma
from .config import Settings
from .http_client import ThrottledSession

log = logging.getLogger("polymarket_pipeline")


def _parse_cli_ts(value: str) -> datetime | None:

    if not value or not value.strip():
        return None
    return gamma.parse_ts(value if "T" in value or " " in value else f"{value}T00:00:00Z")


def resolve_window(
    start: datetime | None, end: datetime | None, watermark: datetime | None, settings: Settings
) -> tuple[datetime, datetime]:

    now = datetime.now(timezone.utc)
    end = min(end or now, now)
    if start is None:
        lookback_start = now - timedelta(days=settings.lookback_days)
        if watermark is None:
            start = lookback_start
        else:
            start = max(watermark - timedelta(minutes=settings.overlap_minutes), lookback_start)
    if start >= end:
        raise SystemExit(f"Invalid window: start {start.isoformat()} is not before end {end.isoformat()}")
    return start, end


def cmd_init_db(settings: Settings, _args) -> int:
    with db.connect(settings) as conn:
        db.ensure_schema(conn, settings)
    log.info("Database schema is up to date")
    return 0


def cmd_ingest_markets(settings: Settings, args) -> int:
    started = datetime.now(timezone.utc)
    session = ThrottledSession(settings.request_interval_seconds)
    with db.connect(settings) as conn:
        db.ensure_schema(conn, settings)
        start, end = resolve_window(
            _parse_cli_ts(args.start), _parse_cli_ts(args.end), db.markets_watermark(conn), settings
        )
        log.info("Ingesting markets closed in [%s, %s)", start.isoformat(), end.isoformat())
        try:
            markets = list(
                gamma.iter_closed_markets(
                    session, settings.gamma_base_url, start, end, settings.min_volume_usd, settings.min_days_open
                )
            )
            loaded = db.upsert_markets(conn, markets, settings.run_id) if markets else 0
        except Exception as exc:
            db.log_run(conn, run_id=settings.run_id, step="markets", status="failed", started_at=started,
                       window_start=start, window_end=end, message=str(exc)[:1000])
            raise
        db.log_run(conn, run_id=settings.run_id, step="markets", status="success", started_at=started,
                   window_start=start, window_end=end, fetched=len(markets), loaded=loaded)
    log.info("Loaded %d market(s) into raw.markets", loaded)
    return 0


def cmd_ingest_prices(settings: Settings, args) -> int:
    started = datetime.now(timezone.utc)
    session = ThrottledSession(settings.request_interval_seconds)
    start, end = _parse_cli_ts(args.start), _parse_cli_ts(args.end)
    if start is not None:
        end = end or datetime.now(timezone.utc)

    with db.connect(settings) as conn:
        db.ensure_schema(conn, settings)
        todo = db.markets_needing_prices(conn, start, end)
        if args.limit:
            todo = todo[: args.limit]
        log.info("Fetching price history for %d market(s)", len(todo))

        loaded, failed = 0, 0
        with ThreadPoolExecutor(max_workers=settings.clob_workers) as pool:
            futures = {
                pool.submit(clob.fetch_price_history, session, settings.clob_base_url, token_id): (market_id, token_id)
                for market_id, token_id in todo
            }
            for i, future in enumerate(as_completed(futures), start=1):
                market_id, token_id = futures[future]
                try:
                    db.save_price_history(conn, market_id, token_id, future.result(), settings.run_id)
                    loaded += 1
                except Exception as exc:  # one bad market must not block the batch
                    failed += 1
                    log.warning("Price history failed for market %s: %s", market_id, exc)
                if i % 100 == 0:
                    log.info("Progress: %d/%d", i, len(todo))

        status = "success" if failed == 0 else ("failed" if loaded == 0 else "partial")
        db.log_run(conn, run_id=settings.run_id, step="prices", status=status, started_at=started,
                   window_start=start, window_end=end, fetched=len(todo), loaded=loaded, failed=failed)
    log.info("Price history: %d loaded, %d failed", loaded, failed)
    return 1 if failed and not loaded else 0


def cmd_transform(settings: Settings, _args) -> int:
    started = datetime.now(timezone.utc)
    with db.connect(settings) as conn:
        db.ensure_schema(conn, settings)
        db.run_sql_files(conn, settings.sql_dir, db.TRANSFORM_FILES)
        rows = conn.execute("select count(*) from marts.fct_market_snapshot").fetchone()[0]
        db.log_run(conn, run_id=settings.run_id, step="transform", status="success", started_at=started, loaded=rows)
    log.info("marts.fct_market_snapshot rebuilt with %d row(s)", rows)
    return 0


def cmd_dq_check(settings: Settings, _args) -> int:
    started = datetime.now(timezone.utc)
    with db.connect(settings) as conn:
        results = conn.execute(
            "select check_name, severity, failing_rows from marts.dq_results order by severity, check_name"
        ).fetchall()
        errors = [r for r in results if r[1] == "error" and r[2] > 0]
        for name, severity, failing in results:
            outcome = "PASS" if failing == 0 else ("FAIL" if severity == "error" else "WARN")
            log.info("%-4s %-40s %-5s failing_rows=%d", outcome, name, severity, failing)
        db.log_run(conn, run_id=settings.run_id, step="dq_check", status="failed" if errors else "success",
                   started_at=started, failed=len(errors),
                   message=", ".join(r[0] for r in errors) or None)
    return 1 if errors else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="polymarket_pipeline", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="create schemas, raw tables and staging views").set_defaults(func=cmd_init_db)

    p = sub.add_parser("ingest-markets", help="load closed markets into raw.markets")
    p.add_argument("--start", default="", help="window start (ISO, inclusive); empty = incremental from watermark")
    p.add_argument("--end", default="", help="window end (ISO, exclusive); empty = now")
    p.set_defaults(func=cmd_ingest_markets)

    p = sub.add_parser("ingest-prices", help="load price history into raw.price_history")
    p.add_argument("--start", default="", help="refetch in-scope markets closed from here (ISO); empty = only missing")
    p.add_argument("--end", default="", help="window end (ISO, exclusive); empty = now")
    p.add_argument("--limit", type=int, default=0, help="fetch at most N markets (0 = no limit)")
    p.set_defaults(func=cmd_ingest_prices)

    sub.add_parser("transform", help="rebuild staging views and marts").set_defaults(func=cmd_transform)
    sub.add_parser("dq-check", help="run data-quality checks").set_defaults(func=cmd_dq_check)
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stdout)
    args = build_parser().parse_args(argv)
    return args.func(Settings(), args)


if __name__ == "__main__":
    sys.exit(main())
