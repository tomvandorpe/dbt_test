with payments as (

    select * from {{ ref('stg_payments') }}
),

total_succesful as (

    select status, count(ID), sum(amount) as total_amount
    from payments
    group by status
)

select * from total_succesful