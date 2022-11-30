SELECT
    raw:tableName::string as table_name, 
    key AS column_name, 
    object_keys(value)[0]::string AS type
FROM JOKO.RAW_SNOWPIPE_INGESTION.POC_INGESTION, 
LATERAL FLATTEN (input => raw:dynamodb:NewImage) 
GROUP BY table_name, key, type
ORDER BY table_name