with payment as (select * from {{ ref('stg_stripe__payments') }}),

orders as (select * from {{ ref('stg_jaffle_shop__orders') }})

select orders.order_id,
orders.customer_id,
payment.amount as amount

from orders

left join payment on orders.order_id = payment.order_id