-- staging
with
    orders_base as ( select * from {{ ref('stg_orders2') }}
        
    ),

    payments_base as (
        select * from {{ ref('stg_payments') }}
    ),

    customer_base as (
        select * from {{ ref('stg_customers2') }}
    ),

    -- intermediate models
    order_totals as (
        select
            order_id,
            max(created) as payment_finalized_date,
            sum(amount) / 100.0 as total_amount_paid
        from payments_base
        where status <> 'fail'
        group by order_id
    ),

    paid_orders as (
        select
            orders.order_id,
            orders.customer_id,
            orders.order_placed_at,
            orders.order_status,
            p.total_amount_paid,
            p.payment_finalized_date,
            customer.customer_first_name,
            customer.customer_last_name
        from orders_base as orders
        left join (order_totals) p on orders.order_id = p.order_id

        left join customer_base as customer on orders.customer_id = customer.id
    ),

    customer_orders as (
        select
            customer_id,
            min(order_placed_at) as first_order_date,
            max(order_placed_at) as most_recent_order_date,
            count(order_id) as number_of_orders
        from orders_base
        group by 1
    )


 

select
    p.*,
    row_number() over (order by p.order_id) as transaction_seq,
    row_number() over (
        partition by customer_id order by p.order_id
    ) as customer_sales_seq,
    case
        when c.first_order_date = p.order_placed_at then 'new' else 'return'
    end as nvsr,
    x.clv_bad as customer_lifetime_value,
    c.first_order_date as fdos
from paid_orders p
left join customer_orders as c using (customer_id)
left outer join
    (
        select p.order_id, sum(t2.total_amount_paid) as clv_bad
        from paid_orders p
        left join
            paid_orders t2
            on p.customer_id = t2.customer_id
            and p.order_id >= t2.order_id
        group by 1
        order by p.order_id
    ) x
    on x.order_id = p.order_id
order by order_id
