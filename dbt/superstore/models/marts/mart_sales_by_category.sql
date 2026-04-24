select
    category,
    sub_category,
    count(distinct order_id)         as total_orders,
    sum(quantity)                    as total_units,
    round(sum(sales), 2)             as total_sales,
    round(sum(profit), 2)            as total_profit,
    round(avg(profit_margin_pct), 2) as avg_margin_pct,
    round(avg(discount_pct), 2)      as avg_discount

from {{ ref('fct_sales') }}
group by category, sub_category
order by total_sales desc
