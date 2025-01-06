select ID as item_ID, order_ID, SKU

from {{ source('jaffle_shop', 'items') }}