-- MEASURE SANITY: abs_amount_egp is an absolute value, so it must be >= 0.
-- A negative here means a corrupted measure slipped through the load.
-- Returns offending rows -> FAILS the test.
select
    transaction_id,
    amount_egp,
    abs_amount_egp
from {{ source('gold', 'fact_transactions') }}
where abs_amount_egp < 0
