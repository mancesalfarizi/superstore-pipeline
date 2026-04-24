{{
    config(
        materialized='incremental',
        unique_key='row_id'
    )
}}

select
    o.row_id,
    o.order_id,
    o.customer_id,
    o.product_id,
    o.order_date,
    o.ship_date,
    o.ship_mode,
    o.days_to_ship,
    o.quantity,
    o.sales,
    o.discount_pct,
    o.discount_tier,
    o.net_sales,
    o.profit,
    round(o.profit / nullif(o.net_sales, 0) * 100, 2) as profit_margin_pct,
    c.segment,
    p.category,
    p.sub_category,
    r.city,
    r.state,
    r.region,
    o._loaded_at

from {{ ref('stg_orders') }} o
left join {{ ref('stg_customers') }}  c on o.customer_id = c.customer_id
left join {{ ref('stg_products') }}   p on o.product_id  = p.product_id
left join {{ ref('raw_superstore') }} r on o.row_id      = r.row_id

{% if is_incremental() %}
    where o.order_date > (
        select max(order_date) from {{ this }}
    )
{% endif %}
