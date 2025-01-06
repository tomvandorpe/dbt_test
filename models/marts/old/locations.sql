

with
location as (select
    store_id,
    city
from {{ ref("stg_stores") }}),

orders_by_location as (

    select
        store_id,
        sum(subtotal) as total_amount,
        sum(order_total) as total_amount_incl_vat,
        max(order_date) as most_recent_order_date,
        min(order_date) as first_order_date,
        (total_amount_incl_vat - total_amount)/total_amount as tax_rate

    from {{ ref("fact_orders") }}

    group by store_id

    order by total_amount

),

orders_by_location_final as (

    select
        orders_by_location.total_amount,
        orders_by_location.total_amount_incl_vat,
        orders_by_location.most_recent_order_date,
        orders_by_location.first_order_date,
        location.city,
        tax_rate

    from location

    left join
        orders_by_location
        on location.store_id = orders_by_location.store_id

    where first_order_date is not null

)

select *
from orders_by_location_final
