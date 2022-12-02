/*
This function takes a string that is defined as camelCase and transforms it into snake_case
*/
{% macro transform_into_snake_case( tableName ) %}
    LOWER(REGEXP_REPLACE( {{tableName}}, '([a-z])([A-Z])', '\\1_\\2'))
{% endmacro %}
