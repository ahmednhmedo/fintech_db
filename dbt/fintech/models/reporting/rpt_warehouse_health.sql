-- Post-load WAREHOUSE HEALTH snapshot (one row) over the gold star.
-- Purpose: a quick reconciliation surface after each incremental MERGE — Power BI
-- (or a reviewer) can confirm at a glance that the fact grew as expected and has
-- no duplicate transactions (total_transactions == distinct_transaction_ids).
-- Pure aggregate read of the gold fact: NO staging / MERGE logic is duplicated.
-- Materialized as a view in the [reporting] schema (see dbt_project.yml).
with f as (
    select * from {{ source('gold', 'fact_transactions') }}
)
select
    count(*)                                       as total_transactions,
    count(distinct transaction_id)                 as distinct_transaction_ids,
    sum(cast(is_outbound as int))                  as outbound_count,
    sum(cast(is_declined as int))                  as declined_count,
    sum(cast(is_fx as int))                        as fx_count,
    min(date_key)                                  as min_date_key,
    max(date_key)                                  as max_date_key,
    cast(sum(abs_amount_egp) as decimal(20, 2))    as gross_volume_egp
from f
