with payments as (
        select
        ID as transaction_seq,
        *
        from {{ source("stripe", "payment") }}
    )

SELECT * from payments


