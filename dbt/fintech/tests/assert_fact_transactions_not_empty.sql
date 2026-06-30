-- ROW-COUNT SANITY (post-incremental-load): the warehouse fact must never be
-- empty. Catches an accidental TRUNCATE / a load that wiped the table instead
-- of upserting. Singular test: PASSES when it returns 0 rows, FAILS otherwise.
select 1 as failure
from (
    select count(*) as n
    from {{ source('gold', 'fact_transactions') }}
) c
where c.n = 0
