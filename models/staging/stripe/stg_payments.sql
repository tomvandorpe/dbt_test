with payments_base as (
        select
        ID as transaction_seq,
        *
        from {{ source("stripe", "payment") }}
    ),


    orders_base as (
        select * from {{ ref('stg_orders') }}
    ),

    orders_payments as (

        select payments.*,
               orders.order_status

        from payments_base as payments

    left join orders_base as orders
    on payments.orderid = orders.id
    )

select * from orders_payments
