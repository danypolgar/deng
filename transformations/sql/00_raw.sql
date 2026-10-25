-- Raw layer: source payloads stored unchanged as JSONB, keyed for idempotent upserts.
-- Applied before every pipeline command; every statement is idempotent.

create schema if not exists raw;
create schema if not exists staging;
create schema if not exists marts;

-- One row per Gamma market (latest version of the record).
create table if not exists raw.markets (
    market_id   text        primary key,
    closed_time timestamptz,             -- copied out of the payload for windowed reads
    payload     jsonb       not null,    -- unchanged Gamma /markets record
    run_id      text        not null,    -- orchestrator execution that last wrote the row
    ingested_at timestamptz not null default now()
);
create index if not exists markets_closed_time_idx on raw.markets (closed_time);

-- One row per market: the full CLOB price series of its Yes token.
create table if not exists raw.price_history (
    market_id   text        primary key,
    token_id    text        not null,
    payload     jsonb       not null,    -- unchanged CLOB /prices-history response
    point_count integer     not null,
    run_id      text        not null,
    ingested_at timestamptz not null default now()
);
-- How often the history was fetched. Later runs retry an empty history until the
-- limit (Settings.max_empty_history_attempts). Added after v0.2, so use ALTER for existing DBs.
alter table raw.price_history add column if not exists fetch_attempts integer not null default 1;

-- One row per pipeline step execution; also provides the incremental watermark.
create table if not exists raw.ingestion_log (
    log_id          bigserial   primary key,
    run_id          text        not null,
    step            text        not null,   -- markets | prices | transform | dq_check
    status          text        not null,   -- success | partial | failed
    window_start    timestamptz,
    window_end      timestamptz,
    records_fetched integer     not null default 0,
    records_loaded  integer     not null default 0,
    records_failed  integer     not null default 0,
    message         text,
    started_at      timestamptz not null,
    finished_at     timestamptz not null default now()
);
-- true = the markets window started at the watermark (scheduled/empty-input run). Only these
-- rows move the watermark, so a manual rerun or backfill window cannot hide a gap before it.
-- Added after v0.2, so use ALTER for existing DBs (old rows count as manual: the next run
-- then backfills LOOKBACK_DAYS once, which is safe because every write is an upsert).
alter table raw.ingestion_log add column if not exists incremental boolean not null default false;
