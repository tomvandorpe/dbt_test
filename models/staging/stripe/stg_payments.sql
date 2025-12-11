with payments_base as (
        select
        ID as transaction_seq,
        *
        from {{ source("stripe", "payments") }}
    ),


    orders_base as (
        select * from {{ ref('stg_orders') }}
    ),

    orders_payments as (

        select payments.*,
               orders.order_status

        from payments_base as payments

    left join orders_base as orders
    on payments.order_id = orders.order_id
    )

select * from orders_payments
