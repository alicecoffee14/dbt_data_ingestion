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


WITH
using_clause AS (

    WITH source_table AS (
    -- here we take the most recent changes coming from the kinesis data stream by looking at stg_stream_logs table
    -- in case of INSERT and MODIFY we take the new image while for DELETES we take the old image (the new one is null) so that 
    -- we can match it with the existing item and flag it with the field 'to_be_deleted' = TRUE
        SELECT
            -- in this part we axtract the fields from raw kinesis logs 
            {% for n in range( table_fields_type|length) %}
                CASE WHEN action_to_perform IN ('INSERT', 'MODIFY') THEN 
                     CAST (raw:dynamodb:NewImage:{{table_fields[n]}}:{{table_fields_type[n]}} AS {{table_fields_type_SF[n]}})
                     WHEN action_to_perform IN ('REMOVE') THEN
                     CAST (raw:dynamodb:OldImage:{{table_fields[n]}}:{{table_fields_type[n]}} AS {{table_fields_type_SF[n]}}) END
                     AS {{table_fields[n]}},                    
            {% endfor %}
            approximate_creation_time, -- we need to add the creation date 
            -- add this flag to remove the deleted items
            CASE WHEN action_to_perform IN ('INSERT', 'MODIFY') THEN FALSE 
                 WHEN action_to_perform IN ('REMOVE') THEN TRUE END
                 AS to_be_deleted

        FROM {{ ref('stg_stream_logs') }}
        WHERE TRUE
            AND table_name = '{{dynamodb_table_name}}'
                
    {% if is_incremental() %}
            -- this filter will only be applied on an incremental run
            /*
            Here we use the approximate_creation_time of the events to increment the table only with the items with
            approximate_creation_time that is after the most recent one when the destination table ({{this}})
            was updated the last time
            Seel below for the definition of 'batch_max_approximate_creation_time'
            */
            AND approximate_creation_time > (SELECT MAX(batch_max_approximate_creation_time) FROM {{ this }})
    )
    SELECT *
    FROM source_table

    {% else %}
    -- this filter will only be applied on full refresh
        /*
        We make an union between the data stream logs and the historic data on which we add by hand an 'approximate_creation_time',
        this date correspond to the last modification of the historic data we want to take. The union is only made on non incremental
        run (namely, in case of full refresh or if the destination table does not exist in the database)
        */ 
        UNION 

        SELECT 
                -- if a new field from kinesis is not in the old table, we need to add it by hand as 'null' to perform the union 
                -- which requires the same number of columns
                {% for n in range( table_fields_type|length) %}
                    {% if table_fields_historic[n] == 'yes' %}
                        {{table_fields[n]}},
                    {% else %}
                        null AS {{table_fields[n]}}, 
                    {% endif %}
               {% endfor %}
               date_part(epoch_millisecond, '2022-12-02 00:00:00.000'::timestamp)::INT AS approximate_creation_time, 
               FALSE AS to_be_deleted
               
        FROM joko.transformed_schema.{{historic_table_name}}
        ), 
    ranking_query AS ( 
        /*
        In the union query we want to select only the most recent changes: if the historic item changed after being inputted in 
        the historic data we only keep the item in the data stream logs. If it never changed we keep the historic one. The ranking
        is made using a partition over the primary key 
        */  
        SELECT *, 
               RANK() OVER ( PARTITION BY 
                                        {% if sort_key != '' %}
                                            ({{primary_key}}, {{sort_key}})
                                        {% else %}
                                            {{primary_key}}
                                        {% endif %}
                                        ORDER BY approximate_creation_time DESC ) AS ordering
        FROM source_table)

    SELECT *
    FROM ranking_query
    WHERE ordering = 1
    {% endif %}

),
-- here the merge statement is applied 
updates AS (

    SELECT
        *
    FROM using_clause

),

inserts AS (

    SELECT 
    *
    FROM using_clause

), 
-- union, without duplicates, of update and insrt statements of the merge 
union_table_after_merge AS (
    SELECT *
    FROM updates

    UNION 

    SELECT * 
FROM inserts), 

-- our incremental strategy is based on the comparison of the approximate_creation_time of new items and the max value of the 
-- approximate_creation_time of the last batch of data that were tadded from kinesis streams
-- it could happen that the last update in the logs was a delete, since we remove it from the table we do not have the correct 
-- batch_max_approximate_creation_time in the table: the workaround is to add a column with the correct batch_max_approximate_creation_time
-- that will be the same for all items in the same loading batch
max_approximate_creation_time AS (
    SELECT max(approximate_creation_time) AS batch_max_approximate_creation_time
    FROM union_table_after_merge
)
-- select only the colums we need and apply a soft delete 
SELECT 
        {% for n in range( table_fields_type|length) %}
            {{table_fields[n]}},
        {% endfor %}
        approximate_creation_time, 
        max_approximate_creation_time.batch_max_approximate_creation_time, 
        to_be_deleted
FROM union_table_after_merge
LEFT JOIN max_approximate_creation_time
-- apply a soft delete because statements like 'if matched DELETE' do not exist in dbt
WHERE TRUE 
     --AND (to_be_deleted != TRUE OR to_be_deleted is null)