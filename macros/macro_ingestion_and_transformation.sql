/*
Macro called to build all the ingestion and transformation model
*/
{% macro ingestion_and_transformation(dynamodb_table_name, historic_table_name, primary_key, sort_key, table_fields, table_fields_type, table_fields_historic, table_fields_type_SF) %}

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
            -- add this flag to remove the deleted items because statements like 'if matched DELETE' do not exist in dbt
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
WHERE TRUE 
     --AND (to_be_deleted != TRUE OR to_be_deleted is null)
        
{% endmacro %}