-- one clean row per fragment from the raw table
with typed as (
    select
        -- order_no arrives as a string so cast it, try_cast gives null if it is not a number
        try_cast(order_no as integer) as order_no,
        -- strip any spaces around the word
        trim(word) as word,
        received_at
    from {{ source('raw', 'fragments') }}
)

select
    order_no,
    word,
    received_at
from typed
-- drop rows that can not be a fragment
where order_no is not null
  and word is not null
  and word <> ''
-- a fragment stored twice counts once, keep the copy received first
qualify row_number() over (partition by order_no order by received_at) = 1
