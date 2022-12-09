/*
This view is built by reading the table of the Kinesis logs (filled by Snowpipe)
some transformation are performed in order to gather informations on the changement implemented
we also extract the table keys (primary and sort keys) and their value in order to create a unique key
made from a concatenation of primary key, sort key and table name: with this unique key we can identify
any unique item in the table and find its most recent update (using a rank statement on the unique primary key
ordered by most recent timestamp) 
*/

WITH find_table_primary_key AS (
    SELECT 
        raw:tableName::string as table_name,  
        OBJECT_KEYS(raw:dynamodb:Keys)[0]::string AS first_key, 
        CASE WHEN ARRAY_SIZE(OBJECT_KEYS(raw:dynamodb:Keys)) = 2 THEN OBJECT_KEYS(raw:dynamodb:Keys)[1]::string END AS second_key,
        OBJECT_KEYS(raw:dynamodb['Keys'][first_key])[0]::string AS type_first_key,
        OBJECT_KEYS(raw:dynamodb['Keys'][second_key])[0]::string AS type_second_key,
        'raw:dynamodb:Keys:'|| first_key || ':' || OBJECT_KEYS(raw:dynamodb['Keys'][first_key])[0]::string AS path_first_key,
        'raw:dynamodb:Keys:'|| second_key || ':' || OBJECT_KEYS(raw:dynamodb['Keys'][second_key])[0]::string AS path_second_key


    FROM {{source ('RAW_SNOWPIPE_INGESTION', 'POC_INGESTION')}}
    GROUP BY table_name, first_key, second_key, type_first_key, type_second_key, path_first_key, path_second_key),
obtain_unique_key AS (
    SELECT T1.*,
           T2.*,
           raw:dynamodb:Keys[T2.first_key][T2.type_first_key]::string AS value_first_key,
           raw:dynamodb:Keys[T2.second_key][T2.type_second_key]::string AS value_second_key, 
           CASE WHEN value_first_key is null THEN T1.raw:tableName::string || value_second_key
                WHEN value_second_key is null THEN T1.raw:tableName::string || value_first_key
                ELSE T1.raw:tableName::string || value_second_key || value_first_key END AS unique_primary_key,
           RANK() OVER ( PARTITION BY unique_primary_key ORDER BY approximate_creation_time DESC ) AS ordering 
    FROM {{source ('RAW_SNOWPIPE_INGESTION', 'POC_INGESTION')}} AS T1
    JOIN find_table_primary_key AS T2 ON T1.raw:tableName::string = T2.table_name
)
-- within the stream we can have different subsequential changes for the same item: we only keep the most recent one
-- to do so we can applied a rank statement descending on creation time and with a partition on the unique primary key
SELECT raw, 
       action AS action_to_perform, 
       approximate_creation_time, 
       table_name, 
       first_key, 
       second_key, 
       value_first_key, 
       value_second_key, 
       ordering
FROM obtain_unique_key
WHERE TRUE
    AND ordering = 1
ORDER BY approximate_creation_time DESC