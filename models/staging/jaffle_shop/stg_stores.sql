select ID as store_ID, NAME as city, opened_at, tax_rate

from {{ source('jaffle_shop', 'stores') }}