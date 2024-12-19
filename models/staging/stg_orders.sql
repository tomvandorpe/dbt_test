

select ID as order_ID, customer as customer_ID, ordered_at as order_date, store_ID, subtotal, tax_paid, order_total

from {{ source('jaffle_shop', 'orders') }}