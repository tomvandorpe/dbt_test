with orders as (select * from {{ ref("stg_orders") }})

select
    order_id,
    customer_id,
    order_date,
    store_id,
    subtotal,
    tax_paid,
    order_total,
    date_part('month', order_date) as order_date_month,
    date_part('year', order_date) as order_date_year
from orders

