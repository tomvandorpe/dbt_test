{%macro example(argument) %}
{%set query%}
select true as boolean
{%end set%}

{%if execute%}
{% set results=run_query(query).columns[0].values()[0] %}
{%endif%}


{%endmacro%}