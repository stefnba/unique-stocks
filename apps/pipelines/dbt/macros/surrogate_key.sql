{% macro surrogate_key(fields) -%}
    md5(
        concat_ws(
            '||',
    {%- for field in fields %}
                coalesce(cast({{ field }} as varchar), '__dbt_null__'){% if not loop.last %},{% endif %}
    {%- endfor %}
        )
    )
{%- endmacro %}
