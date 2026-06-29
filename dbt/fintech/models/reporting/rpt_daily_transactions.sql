-- Example dbt transformation on top of the gold star: a daily transaction
-- summary for Power BI. Materialized as a view in the [reporting] schema.
with f as (
    select * from {{ source('gold', 'fact_transactions') }}
),
d as (
    select date_key, full_date, [year], [month], is_weekend from {{ source('gold', 'dim_date') }}
)
select
    d.full_date,
    d.[year],
    d.[month],
    d.is_weekend,
    count(*)                                         as txn_count,
    sum(cast(f.is_declined as int))                  as declined_count,
    sum(cast(f.is_fx as int))                        as fx_count,
    cast(sum(f.abs_amount_egp) as decimal(18, 2))    as gross_volume_egp,
    cast(avg(f.abs_amount_egp) as decimal(18, 2))    as avg_ticket_egp
from f
join d on d.date_key = f.date_key
group by d.full_date, d.[year], d.[month], d.is_weekend
