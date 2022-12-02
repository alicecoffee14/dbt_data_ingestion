
    -- here we take the most recent changes coming from the kinesis data stream 
        SELECT
            
                CAST (raw:dynamodb:NewImage:email:S AS string) AS email ,
            
                CAST (raw:dynamodb:NewImage:bool:BOOL AS boolean) AS bool ,
            
                CAST (raw:dynamodb:NewImage:userid:S AS string) AS userid ,
            
                CAST (raw:dynamodb:NewImage:age:N AS float) AS age ,
            
                CAST (raw:dynamodb:NewImage:json:M AS variant) AS json ,
            
                CAST (raw:dynamodb:NewImage:test:S AS string) AS test ,
            
            approximate_creation_time -- we need to add the creation date 

        FROM {{ref('stg_stream_logs')}}
        WHERE TRUE
            AND table_name = 'users_test'