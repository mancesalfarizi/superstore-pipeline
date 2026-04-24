select
    segment,
    count(distinct order_id)         as total_orders,
    count(distinct customer_id)      as total_customers,
    round(sum(sales), 2)             as total_sales,
    round(sum(profit), 2)            as total_profit,
    round(avg(profit_margin_pct), 2) as avg_margin_pct,
    round(avg(days_to_ship), 1)      as avg_days_to_ship

from {{ ref('fct_sales') }}
group by segment
order by total_sales desc
