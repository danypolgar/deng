-- Staging layer: parse, type and filter the raw JSON. Views, so they always reflect raw.
-- Applied before every pipeline command; every statement is idempotent.

-- Controlled category vocabulary: free-form Polymarket tag labels -> category.
-- A market gets the category of its highest-priority matching tag (1 wins over 3),
-- so a crypto market that is also tagged "Politics" is classified as crypto.
create table if not exists staging.tag_category (
    tag_label text primary key,   -- lower-case Gamma tag label
    category  text    not null,
    priority  integer not null
);

insert into staging.tag_category (tag_label, category, priority)
values
    ('crypto', 'crypto', 1), ('crypto prices', 'crypto', 1), ('bitcoin', 'crypto', 1),
    ('ethereum', 'crypto', 1), ('solana', 'crypto', 1), ('xrp', 'crypto', 1), ('ripple', 'crypto', 1),
    ('memecoins', 'crypto', 1), ('token launch', 'crypto', 1), ('crypto listings', 'crypto', 1),
    ('economy', 'economics', 2), ('economics', 'economics', 2), ('finance', 'economics', 2),
    ('macro indicators', 'economics', 2), ('inflation', 'economics', 2), ('cpi', 'economics', 2),
    ('us cpi', 'economics', 2), ('fed', 'economics', 2), ('fed rates', 'economics', 2),
    ('global rates', 'economics', 2), ('treasuries', 'economics', 2), ('stocks', 'economics', 2),
    ('equities', 'economics', 2), ('commodities', 'economics', 2), ('oil', 'economics', 2),
    ('gold', 'economics', 2), ('silver', 'economics', 2), ('housing', 'economics', 2),
    ('business', 'economics', 2),
    ('politics', 'politics', 3), ('elections', 'politics', 3), ('global elections', 'politics', 3),
    ('world elections', 'politics', 3), ('geopolitics', 'politics', 3), ('trump', 'politics', 3),
    ('trump presidency', 'politics', 3), ('military actions', 'politics', 3), ('peace deal', 'politics', 3)
on conflict (tag_label) do update set category = excluded.category, priority = excluded.priority;


-- Scope thresholds (single row). Written by the pipeline from config.py (Settings) before
-- every command, so the Python pre-filter and these SQL rules always use the same values.
create table if not exists staging.scope_settings (
    id             boolean     primary key default true check (id),   -- forces exactly one row
    min_volume_usd numeric     not null,
    min_days_open  numeric     not null,
    updated_at     timestamptz not null default now()
);


-- One row per raw market, parsed and typed, with the scope decision and its reason.
create or replace view staging.stg_markets as
with parsed as (
    select
        m.market_id,
        m.payload ->> 'question'                                     as question,
        m.payload ->> 'slug'                                         as slug,
        m.payload ->> 'conditionId'                                  as condition_id,
        m.payload -> 'events' -> 0 ->> 'id'                          as event_id,
        m.payload -> 'events' -> 0 ->> 'title'                       as event_title,
        nullif(m.payload ->> 'startDate', '')::timestamptz           as start_time,
        nullif(m.payload ->> 'endDate', '')::timestamptz             as end_time,
        m.closed_time,
        nullif(m.payload ->> 'volumeNum', '')::numeric               as volume_usd,
        coalesce((m.payload ->> 'negRisk')::boolean, false)          as neg_risk,
        m.payload ->> 'umaResolutionStatus'                          as resolution_status,
        -- these three fields are JSON arrays encoded as strings inside the JSON
        nullif(m.payload ->> 'outcomes', '')::jsonb                  as outcomes,
        nullif(m.payload ->> 'outcomePrices', '')::jsonb             as outcome_prices,
        nullif(m.payload ->> 'clobTokenIds', '')::jsonb              as clob_token_ids,
        array(
            select lower(t ->> 'label')
              from jsonb_array_elements(coalesce(m.payload -> 'tags', '[]'::jsonb)) t
        )                                                            as tags,
        m.ingested_at
    from raw.markets m
),
enriched as (
    select
        p.*,
        p.outcomes = '["Yes", "No"]'::jsonb                          as is_yes_no,
        case
            when p.outcomes = '["Yes", "No"]'::jsonb then p.clob_token_ids ->> 0
        end                                                          as yes_token_id,
        case p.outcome_prices
            when '["1", "0"]'::jsonb then 1
            when '["0", "1"]'::jsonb then 0
        end                                                          as outcome,           -- null = not a clean 0/1 resolution
        p.tags && array['sports', 'esports']                         as is_sports,
        coalesce((
            select tc.category
              from staging.tag_category tc
             where tc.tag_label = any (p.tags)
             order by tc.priority
             limit 1
        ), 'other')                                                  as category,
        extract(epoch from p.closed_time - p.start_time) / 86400.0   as days_open
    from parsed p
),
scoped as (
select
    e.market_id, e.event_id, e.event_title, e.question, e.slug, e.condition_id,
    e.category, e.tags, e.start_time, e.end_time, e.closed_time, e.days_open,
    e.volume_usd, e.neg_risk, e.resolution_status, e.outcomes, e.outcome_prices,
    e.yes_token_id, e.outcome,
    -- Scope rules (README section 3). Order matters: the first failing rule is reported.
    case
        when not coalesce(e.is_yes_no, false)            then 'not_binary_yes_no'
        when e.is_sports                                 then 'sports'
        when e.category = 'other'                        then 'out_of_scope_category'
        when e.resolution_status is distinct from 'resolved' then 'not_resolved'
        when e.outcome is null                           then 'unclean_resolution'
        when coalesce(e.volume_usd, 0) < cfg.min_volume_usd then 'low_volume'
        when e.days_open is null or e.days_open < cfg.min_days_open then 'open_less_than_30_days'
        when e.yes_token_id is null                      then 'missing_token_id'
    end                                                  as exclusion_reason,
    e.ingested_at
from enriched e
cross join staging.scope_settings cfg
)
select s.*, s.exclusion_reason is null as in_scope
from scoped s;


-- One row per market and price point of its Yes token.
create or replace view staging.stg_price_history as
select
    ph.market_id,
    ph.token_id,
    to_timestamp((h ->> 't')::bigint) as price_ts,
    (h ->> 'p')::numeric              as price          -- Yes price = implied probability
from raw.price_history ph
cross join lateral jsonb_array_elements(coalesce(ph.payload -> 'history', '[]'::jsonb)) h;
