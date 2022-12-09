/*
Parameters of the incremental model:
- unique_key: determines whether a record has new values and should be updated. 
  If the same unique_key is present in the "new" and "old" model data, dbt will update/replace the old row with the new row 
  of data. The exact mechanics of how that update/replace takes place will vary depending on your database and incremental strategy.
  If the unique_key is not present in the "old" data, dbt will insert the entire row into the table.
- incremental_strategy: 
- on_schema_change: enable additional control when incremental model columns change. append_new_columns: Append new columns 
  to the existing table. Note that this setting does not remove columns from the existing table that are not present in the 
  new data. There is no backfill values in old records for newly added columns. 
*/
{{
    config(
        materialized='incremental',
        unique_key = 'userid',
        incremental_strategy='merge', 
        on_schema_change='append_new_columns'
    )
}}


WITH
using_clause AS (

    WITH source_table AS (
    -- these are the most recent changes coming from the kinesis data stream
        SELECT
            raw:dynamodb:NewImage:userid:S::string as userid,
            raw:dynamodb:NewImage:age:N::string as age, 
            raw:dynamodb:NewImage:email:S::string as email, 
            raw:dynamodb:NewImage:test:S::string as test,
            raw:dynamodb:NewImage:json:M as json,
            raw:dynamodb:NewImage:bool:BOOL::boolean as bool, 
            approximate_creation_time -- we need to add the creation date only to merge recent items

        FROM {{ ref('stg_stream_logs') }}
        WHERE TRUE
            AND table_name = 'users_test'
                
        {% if is_incremental() %}
            -- this filter will only be applied on an incremental run
            /*
            Here we use the approximate_creation_time of the events to increment the table only with the items with
            approximate_creation_time that is after the most recent one when the destination table ({{this}})
            was updated the last time
            */
            AND approximate_creation_time > (SELECT max(approximate_creation_time) FROM {{ this }})
    )
    SELECT *
    FROM source_table

    {% else %}
    -- this filter will only be applied on full refresh
        /*
        We make an union between the data stream logs and the historic data on which we add by hand an 'approximate_creation_time',
        this date correspond to the last modification of the historic data we want to take 
        */ 
        UNION 

        SELECT *,
               date_part(epoch_millisecond, current_timestamp())::INT AS approximate_creation_time
        FROM joko.transformed_schema.users_test
        ), 
    ranking_query AS ( 
        /*
        In the union query we want to select only the most recent changes: if the historic item changed after being inputted in 
        the historic data we only keep the item in the data strem logs. If it never changed we keep the historic one. The ranking
        is made using a partition over the primary key 
        */  
        SELECT *, 
               RANK() OVER ( PARTITION BY userid ORDER BY approximate_creation_time DESC ) AS ordering
        FROM source_table)
    SELECT *
    FROM ranking_query
    WHERE ordering = 1
    {% endif %}

),

updates AS (

    SELECT
        *

    FROM using_clause

),

inserts AS (

    SELECT 
    *

    FROM using_clause

)

SELECT *
FROM updates

UNION 

SELECT *
FROM inserts
