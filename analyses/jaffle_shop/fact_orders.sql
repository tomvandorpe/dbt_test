with orders as (select * from {{ ref("stg_orders") }})

select
    order_id,
    customer_id,
    order_date,
    store_id,
    subtotal,
    tax_paid,
    order_total
from orders

