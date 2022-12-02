/*
This table contains the information on the table's keys
*/
SELECT
raw:tableName::string as source_table_name, 
{{transform_into_snake_case('source_table_name')}} AS destination_table_name,
key AS keys_name, 
object_keys(value)[0]::string AS kinesis_type

    
FROM {{source ('RAW_SNOWPIPE_INGESTION', 'POC_INGESTION')}}, 
LATERAL FLATTEN (input => raw:dynamodb:Keys) 
GROUP BY source_table_name, destination_table_name, key, kinesis_type
ORDER BY source_table_name