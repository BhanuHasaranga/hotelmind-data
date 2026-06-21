-- Override dbt's default schema name behavior.
-- Without this, dbt appends custom_schema to target_schema, producing names like
-- "hotelmind_dw_hotelmind_warehouse". This macro uses the custom schema directly.
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
