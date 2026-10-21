-- Calibration summary for the analyst.
-- Grain: one category x horizon x 10% probability bucket, plus category = 'all'
-- rows that pool every category.
-- A well-calibrated market has mean_implied_prob close to observed_frequency.

create or replace view marts.mart_calibration_summary as
select
    coalesce(category, 'all')            as category,
    horizon_days,
    prob_bucket,
    count(*)                             as market_count,
    round(avg(implied_prob), 4)          as mean_implied_prob,
    round(avg(outcome), 4)               as observed_frequency,
    round(avg(outcome) - avg(implied_prob), 4) as calibration_gap,   -- > 0: event happened more often than priced
    round(avg(squared_error), 4)         as brier_score
from marts.fct_market_snapshot
group by grouping sets ((category, horizon_days, prob_bucket), (horizon_days, prob_bucket));
