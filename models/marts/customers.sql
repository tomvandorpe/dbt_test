
with customers as (

    select * from {{ ref('stg_customers') }}

),

orders as (select * from {{ ref('fact_orders') }} ),


customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders,
        sum(subtotal) as total_amount,
        sum(order_total) as total_amount_incl_vat,
        avg(order_total) as average_amount

    from orders




    group by customer_id

),


customer_orders_final as (

select customers.name, customer_orders.first_order_date, 
customer_orders.most_recent_order_date, coalesce(customer_orders.number_of_orders,0) as number_of_orders,
customer_orders.total_amount, customer_orders.total_amount_incl_vat, customer_orders.average_amount

from customers

Left Join customer_orders 

on customers.customer_ID = customer_orders.customer_ID


order by number_of_orders asc

)




select * from customer_orders_final