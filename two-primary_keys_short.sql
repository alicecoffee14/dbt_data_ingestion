/*------------------------------------------------------------------------------------------------------*/
/* Parameters to automatise the query, specific of each table */
/*-----------------------------------------------------------------------*/

-- parameters to set differently for each table: these must be set by hand
{% set dynamodb_table_name = 'two_primary_keys' %}
{% set historic_table_name = 'two_primary_keys' %}
{% set primary_key = 'userid' %}
{% set sort_key = 'timestamp' %}

-- these below can be set by hand if needed but they are automatically computed below
--{% set table_fields = ['userid', 'timestamp', 'test'] %}
--{% set table_fields_type = ['S', 'N', 'S'] %}
--{% set table_fields_type_SF = ['string', 'float', 'string'] %}

/*-----------------------------------------------------------------------*/
-- BELOW HERE: DO NOT CHANGE 
-- functions that computes the remaining parameters

-- here we create the lists of table_fields, table_fields_type and table_fields_historic by running a query on
-- the view 'table_columns'
-- table_fields = name of the fields in the dynamodb table
-- table_fields_type = type of the table_fields in kinesis logs
-- table_fields_histoic = yes/no depending if the table_field was already present in the historic tables
{% set column_name_query %}
SELECT column_name, 
       kinesis_type, 
       source_table_name, 
       destination_table_name, 
       is_this_column_in_historic_table 
FROM {{ref('stg_table_columns')}}
WHERE destination_table_name = '{{dynamodb_table_name}}'
{% endset %}
{% set results = run_query(column_name_query) %}

{% if execute %}
        {% set table_fields = results.columns[0].values() %}
        {% set table_fields_type = results.columns[1].values() %}
        {% set table_fields_historic = results.columns[4].values() %}
{% endif %}

-- here we define the unique primary keys that are used in the transformation jobs 
-- there is a different treatement depening if there is a sort key or not
{% if sort_key != '' %}
    {% set unique_primary_key = [ primary_key , sort_key ]  %}
{% else %}
    {% set  unique_primary_key = primary_key %}
{% endif %}

-- define the types to use in Snowflake using a dictionary between Kinesis types and SF types
-- the function is contained in the macro called 'get_SF_types'
{% set table_fields_type_SF =  get_SF_types(table_fields_type) %}

/*------------------------------------------------------------------------------------------------------*/
/*
Parameters of the incremental model:
- unique_key: determines whether a record has new values and should be updated. 
  If the same unique_key is present in the "new" and "old" model data, dbt will update/replace the old row with the new row 
  of data. The exact mechanics of how that update/replace takes place will vary depending on incremental strategy.
  If the unique_key is not present in the "old" data, dbt will insert the entire row into the table.
- incremental_strategy: with the merge strategy with specified a unique_key, by default, dbt will entirely overwrite 
  matched rows with new values.
- on_schema_change: enable additional control when incremental model columns change. append_new_columns: append new columns 
  to the existing table. Note that this setting does not remove columns from the existing table that are not present in the 
  new data. There is no backfill values in old records for newly added columns but it can be done with a full refresh run. 
*/
{{
    config(
        materialized='incremental',
        unique_key = unique_primary_key,
        incremental_strategy='merge', 
        on_schema_change='append_new_columns'
    )
}}
/*------------------------------------------------------------------------------------------------------*/
/*    Call the macro of the incremental model for ingestion and transformation                          */
{{ short(dynamodb_table_name, historic_table_name, primary_key, sort_key, table_fields, table_fields_type, table_fields_historic, table_fields_type_SF) }}