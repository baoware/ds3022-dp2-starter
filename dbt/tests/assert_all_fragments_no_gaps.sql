-- fails unless there are exactly the expected number of fragments, numbered 1 to N with no gaps
-- a singular test fails when it returns rows, so this returns one row only when something is wrong
with stats as (
    select
        count(*) as fragments,
        count(distinct order_no) as distinct_order_nos,
        min(order_no) as lowest,
        max(order_no) as highest
    from {{ ref('stg_fragments') }}
)

select *
from stats
-- N distinct numbers that start at 1 and end at N can not have a gap
where fragments <> {{ var('expected_fragments') }}
   or distinct_order_nos <> {{ var('expected_fragments') }}
   or lowest is distinct from 1
   or highest is distinct from {{ var('expected_fragments') }}
