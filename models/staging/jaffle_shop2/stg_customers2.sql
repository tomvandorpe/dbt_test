with
customer_base as (
    select
        id,
        first_name as customer_first_name,
        last_name as customer_last_name
    from {{ source("jaffle_shop2", "customers") }}
)

select *
from customer_base
