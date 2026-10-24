import os
from dataclasses import dataclass, field
from pathlib import Path

_DEFAULT_SQL_DIR = Path(__file__).resolve().parents[2] / "transformations" / "sql"


def _env(name: str, default: str) -> str:
    value = os.environ.get(name, "").strip()
    return value or default


@dataclass(frozen=True)
class Settings:

    pg_host: str = field(default_factory=lambda: _env("PG_HOST", "localhost"))
    pg_port: int = field(default_factory=lambda: int(_env("PG_PORT", "5432")))
    pg_database: str = field(default_factory=lambda: _env("PG_DATABASE", "polymarket"))
    pg_user: str = field(default_factory=lambda: _env("PG_USER", "polymarket"))
    pg_password: str = field(default_factory=lambda: _env("PG_PASSWORD", ""))

    gamma_base_url: str = field(default_factory=lambda: _env("GAMMA_BASE_URL", "https://gamma-api.polymarket.com"))
    clob_base_url: str = field(default_factory=lambda: _env("CLOB_BASE_URL", "https://clob.polymarket.com"))
    request_interval_seconds: float = field(
        default_factory=lambda: float(_env("REQUEST_INTERVAL_SECONDS", "0.15"))
    )
    clob_workers: int = field(default_factory=lambda: int(_env("CLOB_WORKERS", "4")))

    # DNS-over-HTTPS for the API hosts (works around Swiss ISP DNS blocks). Empty DOH_URL = off.
    doh_url: str = field(default_factory=lambda: os.environ.get("DOH_URL", "https://1.1.1.1/dns-query").strip())

    lookback_days: int = field(default_factory=lambda: int(_env("LOOKBACK_DAYS", "30")))
    overlap_minutes: int = field(default_factory=lambda: int(_env("OVERLAP_MINUTES", "15")))

    # Scope thresholds. Single source of truth: ensure_schema() writes them into
    # staging.scope_settings, which staging.stg_markets reads. The Gamma pre-filter uses the
    # same values. Constants on purpose: every task must see the same values.
    min_volume_usd: float = 10_000
    min_days_open: int = 30

    # An in-scope market whose CLOB history comes back empty is fetched again by later
    # runs until it has been tried this many times.
    max_empty_history_attempts: int = 3

    sql_dir: Path = field(default_factory=lambda: Path(_env("SQL_DIR", str(_DEFAULT_SQL_DIR))))
    run_id: str = field(default_factory=lambda: _env("RUN_ID", "manual"))

    @property
    def pg_dsn(self) -> str:
        return (
            f"host={self.pg_host} port={self.pg_port} dbname={self.pg_database} "
            f"user={self.pg_user} password={self.pg_password}"
        )
