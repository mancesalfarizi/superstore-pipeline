select
    region,
    state,
    count(distinct order_id)         as total_orders,
    sum(quantity)                    as total_units,
    round(sum(sales), 2)             as total_sales,
    round(sum(net_sales), 2)         as total_net_sales,
    round(sum(profit), 2)            as total_profit,
    round(avg(profit_margin_pct), 2) as avg_margin_pct

from {{ ref('fct_sales') }}
group by region, state
order by total_sales desc
