with payments as (
        select
        orderid as order_id,
        ID as transaction_seq,
        {{cents_to_dollars("amount", 5)}} as amount,
        paymentmethod as payment_method,
        status,
        created as created_at
        from {{ source("stripe", "payment") }}
    )

SELECT * from payments


