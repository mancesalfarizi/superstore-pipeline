{{
    config(
        materialized='incremental',
        unique_key='row_id'
    )
}}

select
    order_id,
    customer_id,
    product_id,
    row_id,
    to_date(order_date_raw, 'MM/DD/YYYY')    as order_date,
    to_date(ship_date_raw, 'MM/DD/YYYY')     as ship_date,
    ship_mode,
    quantity_raw                              as quantity,
    sales_raw                                 as sales,
    discount_raw                              as discount_pct,
    profit_raw                                as profit,
    round(sales_raw * (1 - discount_raw), 2)  as net_sales,
    (to_date(ship_date_raw, 'MM/DD/YYYY') -
     to_date(order_date_raw, 'MM/DD/YYYY'))::integer as days_to_ship,
    case
        when discount_raw = 0     then 'No Discount'
        when discount_raw <= 0.2  then 'Low Discount'
        when discount_raw <= 0.4  then 'Medium Discount'
        else                           'High Discount'
    end                                       as discount_tier,
    _loaded_at

from {{ ref('raw_superstore') }}

{% if is_incremental() %}
    where to_date(order_date_raw, 'MM/DD/YYYY') > (
        select max(order_date) from {{ this }}
    )
{% endif %}
