select distinct
    city,
    state,
    country,
    postal_code,
    region

from {{ ref('raw_superstore') }}
where city is not null
