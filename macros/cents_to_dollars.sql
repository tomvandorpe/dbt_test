{%- macro cents_to_dollars(test,decimal_spaces) -%}

round( {{test}} /100, {{decimal_spaces}} )

{%- endmacro %}