{%macro cents_to_dollars(column, decimals=2)-%}
round({{column}}/100,{{decimals}})
{%-endmacro%}