
select ID as customer_ID, name

from {{ source('jaffle_shop', 'customers') }}