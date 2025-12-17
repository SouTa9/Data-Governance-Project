{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- if custom_schema_name is none -%}
        {# Use the default schema (BRONZE) for models that don't have a custom schema #}
        {{ target.schema }}
    {%- else -%}
        {# Use the custom schema specified in dbt_project.yml or model config #}
        {{ custom_schema_name | upper }}
    {%- endif -%}

{%- endmacro %}
