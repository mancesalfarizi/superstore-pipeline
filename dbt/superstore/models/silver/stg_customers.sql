select distinct
    customer_id,
    customer_name,
    segment

from {{ ref('raw_superstore') }}
where customer_id is not null
