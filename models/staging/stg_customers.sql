
{{
    config(
        materialized='table'
    )
}}

select ID as customer_ID, FIRST_NAME, LAST_NAME

from {{ source('jaffle_shop', 'customers') }}