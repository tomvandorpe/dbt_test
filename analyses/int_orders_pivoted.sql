with payments as (
    select * from {{ ref('stg_payments') }}
),

pivoted as (
    select
        order_id,

        {% set payment_methods = ['bank_transfer', 'coupon', 'credit_card', 'gift_card'] %}

        {% for method in payment_methods %}

            sum(
                case
                    when
                        payment_method = '{{ method }}'
                        then {{ cents_to_dollars('amount',2) }} else 0
                end
            )
                as {{ method }}_amount
    
            {%- if loop.last -%}

            {%-else-%}
                ,
            {%- endif -%}

        {%- endfor %}

    from payments
    where order_status = 'completed'
    group by order_id


)

select * from pivoted
