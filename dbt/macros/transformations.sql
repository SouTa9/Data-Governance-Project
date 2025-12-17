{% macro normalize_text(column_name) -%}
    nullif(trim({{ column_name }}), '')
{%- endmacro %}

{% macro normalize_upper(column_name) -%}
    upper({{ normalize_text(column_name) }})
{%- endmacro %}

{% macro safe_divide(numerator, denominator, default=0) -%}
    case
        when {{ denominator }} = 0 or {{ denominator }} is null then {{ default }}
        else {{ numerator }} / {{ denominator }}
    end
{%- endmacro %}

{% macro surrogate_key(columns) -%}
    {%- set expressions = [] -%}
    {%- for column in columns -%}
        {%- set _ = expressions.append("coalesce(cast(" ~ column ~ " as varchar), '')") -%}
    {%- endfor -%}
    md5(
        {{ expressions | join(" || '||' || ") }}
    )
{%- endmacro %}
