

with location as (select store_ID, city from {{ ref('stg_stores') }}),

orders_by_location as (
    
select store_ID, sum(subtotal) as total_amount, sum(order_total) as total_amount_incl_VAT,
max(order_date) as most_recent_order_date, min(order_date) as first_order_date

from {{ ref('fact_orders') }}

group by store_ID

order by total_amount

),

orders_by_location_final as(
    
select orders_by_location.total_amount, orders_by_location.total_amount_incl_VAT, 
orders_by_location.most_recent_order_date, orders_by_location.first_order_date, location.city

from location

left join orders_by_location

on location.store_ID = orders_by_location.store_ID

WHERE first_order_date is not null

)

select * from orders_by_location_final