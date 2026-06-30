{{ config(severity = 'warn') }}
-- BUSINESS-RULE SANITY: a declined transaction (is_declined = 1) should carry a
-- decline_reason_key. Severity = warn (not error) because it is a data-quality
-- expectation, not a hard warehouse constraint -- a violation is worth flagging
-- but should not fail the whole build. Returns offending rows -> WARNS.
select
    transaction_id,
    is_declined,
    decline_reason_key
from {{ source('gold', 'fact_transactions') }}
where is_declined = 1
  and decline_reason_key is null
