{#{{
    config(
        materialized='incremental',
        unique_key = 'order_id',
        incremental_strategy = 'merge',
    )
}}#}


with payments as (select * from {{ ref('stg_stripe__payments') }}),

orders as (select * from {{ ref('stg_jaffle_shop__orders') }}),

order_payments as (
    select
        order_id,
        sum(case when status = 'success' then amount end) as amount

    from payments
    group by 1
),

 final as (

    select
        orders.order_id,
        orders.customer_id,
        orders.order_placed_at,
        coalesce (order_payments.amount, 0) as amount

    from orders
    left join order_payments using (order_id)
)



select * from final
{# {% if is_incremental() %}
where
order_placed_at > (select max(order_placed_at) from {{this}})
{% endif %}
order by order_placed_at desc #}
