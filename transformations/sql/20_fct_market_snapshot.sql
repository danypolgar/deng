-- Core table for calibration research.
-- Grain: one in-scope resolved market x one horizon (30, 7 or 1 days before close).
--
-- implied_prob is the last Yes-token price observed strictly BEFORE the horizon
-- timestamp (closed_time - horizon), so no information from after the horizon leaks
-- in. Prices older than 3 days at the horizon are treated as missing (illiquid
-- market) and the market x horizon row is left out.
--
-- Rebuilt in full on every run (the data is small); because the transform runs in a
-- single transaction, readers never see a half-built table and reruns are idempotent.

create table if not exists marts.fct_market_snapshot (
    market_id       text          not null,
    horizon_days    integer       not null,
    event_id        text,
    question        text          not null,
    category        text          not null,
    closed_time     timestamptz   not null,
    horizon_ts      timestamptz   not null,   -- closed_time - horizon_days
    price_ts        timestamptz   not null,   -- time of the price used
    staleness_hours numeric       not null,   -- horizon_ts - price_ts
    implied_prob    numeric       not null,   -- Yes price in [0, 1]
    prob_bucket     numeric       not null,   -- lower bound of the 10% bucket: 0.0, 0.1, ..., 0.9
    outcome         integer       not null,   -- 1 = resolved Yes, 0 = resolved No
    squared_error   numeric       not null,   -- (implied_prob - outcome)^2, averages to the Brier score
    volume_usd      numeric,
    built_at        timestamptz   not null default now(),
    primary key (market_id, horizon_days)
);

delete from marts.fct_market_snapshot;

insert into marts.fct_market_snapshot (
    market_id, horizon_days, event_id, question, category, closed_time, horizon_ts,
    price_ts, staleness_hours, implied_prob, prob_bucket, outcome, squared_error, volume_usd
)
with horizons as (
    select m.*, h.horizon_days, m.closed_time - make_interval(days => h.horizon_days) as horizon_ts
      from staging.stg_markets m
     cross join (values (30), (7), (1)) as h (horizon_days)
     where m.in_scope
),
last_price as (
    select
        h.market_id,
        h.horizon_days,
        p.price_ts,
        p.price,
        row_number() over (partition by h.market_id, h.horizon_days order by p.price_ts desc) as rn
    from horizons h
    join staging.stg_price_history p
      on p.market_id = h.market_id
     and p.price_ts <  h.horizon_ts
     and p.price_ts >= h.horizon_ts - interval '3 days'
)
select
    h.market_id,
    h.horizon_days,
    h.event_id,
    h.question,
    h.category,
    h.closed_time,
    h.horizon_ts,
    lp.price_ts,
    round(extract(epoch from h.horizon_ts - lp.price_ts) / 3600.0, 2),
    lp.price,
    least(floor(lp.price * 10) / 10, 0.9),
    h.outcome,
    power(lp.price - h.outcome, 2),
    h.volume_usd
from horizons h
join last_price lp
  on lp.market_id = h.market_id and lp.horizon_days = h.horizon_days and lp.rn = 1;
