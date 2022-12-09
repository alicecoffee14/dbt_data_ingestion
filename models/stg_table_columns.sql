/*
This table contains the information on the table schema in our database
The first part extracts the fields, and their types, in each table in from Kinesis logs. However 
we need to know if the historic table, namely the table we already have on Snowflake before the start of the 
incremental loading, have the same fields: with the time we can add new columns to the table that will not be
in the historic table, we need to differentiate them to avoid error if we do a UNION statement between
Kinesis logs and historic table (UNION requires the same number of columns). To do so, we take the historic
table schema from the table 'historic_tables_schema', created via a SF query
*/

WITH kinesis_tables AS (
    SELECT
    raw:tableName::string as source_table_name, 
    {{transform_into_snake_case('source_table_name')}} AS destination_table_name,
    key AS column_name, 
    object_keys(value)[0]::string AS kinesis_type

        
    FROM {{source ('RAW_SNOWPIPE_INGESTION', 'POC_INGESTION')}}, 
    LATERAL FLATTEN (input => raw:dynamodb:NewImage) 
    GROUP BY source_table_name, destination_table_name, key, kinesis_type
    ORDER BY source_table_name
)
SELECT 
    T1.source_table_name,
    T1.destination_table_name,  
    T1.column_name, 
    CASE WHEN T2.column_name IS NOT NULL THEN 'yes' 
         ELSE 'no' END AS is_this_column_in_historic_table, 
    T1.kinesis_type
FROM kinesis_tables AS T1
LEFT JOIN {{source ('TRANSFORMED_SCHEMA', 'historic_tables_schema')}} AS T2 
    ON lower(T1.destination_table_name) = lower(T2.table_name) AND lower(T1.column_name) = lower(T2.column_name)
ORDER BY source_table_name