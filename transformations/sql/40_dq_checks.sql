-- Data-quality checks. One row per check with the number of failing rows.
-- severity 'error' fails the pipeline (dq-check exits 1); 'warn' is reported only.

create or replace view marts.dq_results as
select 'snapshot_grain_unique' as check_name, 'error' as severity,
       count(*) - count(distinct (market_id, horizon_days)) as failing_rows
  from marts.fct_market_snapshot
union all
select 'snapshot_implied_prob_in_0_1', 'error', count(*)
  from marts.fct_market_snapshot where implied_prob not between 0 and 1
union all
select 'snapshot_outcome_is_0_or_1', 'error', count(*)
  from marts.fct_market_snapshot where outcome not in (0, 1)
union all
select 'snapshot_no_lookahead', 'error', count(*)
  from marts.fct_market_snapshot where price_ts >= horizon_ts
union all
select 'snapshot_horizon_in_allowed_set', 'error', count(*)
  from marts.fct_market_snapshot where horizon_days not in (30, 7, 1)
union all
select 'stg_price_in_0_1', 'error', count(*)
  from staging.stg_price_history where price not between 0 and 1
union all
-- stg_markets cross-joins the single settings row; without it every market silently disappears.
select 'scope_settings_single_row', 'error', abs(count(*) - 1)
  from staging.scope_settings
union all
select 'in_scope_market_without_price_history', 'warn', count(*)
  from staging.stg_markets m
 where m.in_scope and not exists (select 1 from raw.price_history p where p.market_id = m.market_id)
union all
select 'in_scope_market_with_empty_price_history', 'warn', count(*)
  from staging.stg_markets m join raw.price_history p using (market_id)
 where m.in_scope and p.point_count = 0
union all
select 'in_scope_market_without_any_snapshot', 'warn', count(*)
  from staging.stg_markets m
 where m.in_scope
   and exists (select 1 from raw.price_history p where p.market_id = m.market_id)
   and not exists (select 1 from marts.fct_market_snapshot s where s.market_id = m.market_id);
