/*
This function takes a list of types as they are written in Kinesis data logs (S, M, BOOl...) and converts them into
Snowflake types (string, varchar, float...) using a dictionary
Note that all numbers are converted into float (no distinction between INT and FLOAT in Kinesis)
*/
{% macro get_SF_types( table_fields_type ) %}
{% set type_map = ({"S":"string", "N":"float", "M":"variant", "BOOL":"boolean"}) %}
{% set empty_list = [] %} 
    {%for item in table_fields_type %} 
        {%  set empty_list = empty_list.append(type_map[item]) %}
    {% endfor %} 
{{ return(empty_list) }}
{% endmacro %}

-- old piece of code that I wrote in the set parameters part of two_primary_keys (just for backup)
-- define the types to use in Snowflake using a dictionary between Kinesis types and SF types
{#
--{% set type_map = ({"S":"string", "N":"float", "M":"variant", "BOOL":"boolean"}) %}
--{% set empty_list = [] %} 
--    {%for item in table_fields_type %} 
--        {%  set empty_list = empty_list.append(type_map[item]) %}
--    {% endfor %} 
--{% set table_fields_type_SF = empty_list %}
#}