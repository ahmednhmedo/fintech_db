-- MEASURE INTEGRITY: by definition abs_amount_egp = ABS(amount_egp). This
-- reconciles the two money columns after the incremental load, catching any
-- row where the absolute measure drifted from the signed measure.
-- Returns offending rows -> FAILS the test.
select
    transaction_id,
    amount_egp,
    abs_amount_egp
from {{ source('gold', 'fact_transactions') }}
where abs_amount_egp <> abs(amount_egp)
