with payments_base as (
        select orderid as order_id,
        ID as transaction_seq,
        *
        from {{ source("stripe", "payments") }}
    )

select * from payments_base
