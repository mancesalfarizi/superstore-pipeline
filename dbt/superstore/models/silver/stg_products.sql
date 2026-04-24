with source as (
    select
        product_id,
        product_name,
        category,
        sub_category,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY product_name
        ) as rn
    from {{ ref('raw_superstore') }}
    where product_id is not null
)

select
    product_id,
    product_name,
    category,
    sub_category

from source
where rn = 1
