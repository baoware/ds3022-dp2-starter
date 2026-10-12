-- one row holding the phrase, the words joined in order_no order
select
    string_agg(word, ' ' order by order_no) as phrase,
    count(*) as words
from {{ ref('stg_fragments') }}
