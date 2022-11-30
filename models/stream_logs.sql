WITH find_table_primary_key AS (
    SELECT 
        raw:tableName::string as table_name,  
        OBJECT_KEYS(raw:dynamodb:Keys)[0]::string AS first_key, 
        CASE WHEN ARRAY_SIZE(OBJECT_KEYS(raw:dynamodb:Keys)) = 2 THEN OBJECT_KEYS(raw:dynamodb:Keys)[1]::string END AS second_key,
        OBJECT_KEYS(raw:dynamodb['Keys'][first_key])[0]::string AS type_first_key,
        OBJECT_KEYS(raw:dynamodb['Keys'][second_key])[0]::string AS type_second_key,
        'raw:dynamodb:Keys:'|| first_key || ':' || OBJECT_KEYS(raw:dynamodb['Keys'][first_key])[0]::string AS path_first_key,
        'raw:dynamodb:Keys:'|| second_key || ':' || OBJECT_KEYS(raw:dynamodb['Keys'][second_key])[0]::string AS path_second_key


    FROM JOKO.RAW_SNOWPIPE_INGESTION.POC_INGESTION
    GROUP BY table_name, first_key, second_key, type_first_key, type_second_key, path_first_key, path_second_key),
obtain_unique_key AS (
    SELECT T1.*,
           T2.*,
           raw:dynamodb:Keys[T2.first_key][T2.type_first_key]::string AS value_first_key,
           raw:dynamodb:Keys[T2.second_key][T2.type_second_key]::string AS value_second_key, 
           CASE WHEN value_first_key is null THEN value_second_key
                WHEN value_second_key is null THEN value_first_key
                ELSE value_second_key || value_first_key END AS unique_primary_key,
           RANK() OVER ( PARTITION BY unique_primary_key ORDER BY approximate_creation_time DESC ) AS ordering 
    FROM JOKO.RAW_SNOWPIPE_INGESTION.POC_INGESTION AS T1
    JOIN find_table_primary_key AS T2 ON T1.raw:tableName::string = T2.table_name
)
-- within the stream we can have different subsequential changes for the same item: we only keep the most recent one
-- to do so we can applied a rank statement descending on creation time and with a partition on the primary key
SELECT *
FROM obtain_unique_key
WHERE TRUE
    AND ordering = 1