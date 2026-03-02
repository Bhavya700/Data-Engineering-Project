{{ config(materialized='view') }}

with raw_pulls as (
    select * from {{ source('github', 'github_pulls_raw') }}
)

/* 
  Example of flattening a raw JSON payload in BigQuery.
  Assuming the raw_data column contains the full event JSON.
  Adjust the JSON_EXTRACT_SCALAR or dot notation depending on how Airflow ingested it.
*/
select
    -- Use JSON_EXTRACT_SCALAR to extract fields from JSON column
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.id') as INT64) as pull_request_id,
    JSON_EXTRACT_SCALAR(raw_data, '$.node_id') as pull_request_node_id,
    JSON_EXTRACT_SCALAR(raw_data, '$.state') as pull_request_state,
    JSON_EXTRACT_SCALAR(raw_data, '$.title') as pull_request_title,
    
    -- Parse dates
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.created_at') as TIMESTAMP) as created_at,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.updated_at') as TIMESTAMP) as updated_at,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.closed_at') as TIMESTAMP) as closed_at,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.merged_at') as TIMESTAMP) as merged_at,
        
    -- User details
    JSON_EXTRACT_SCALAR(raw_data, '$.user.login') as author_login,
    SAFE_CAST(JSON_EXTRACT_SCALAR(raw_data, '$.user.id') as INT64) as author_id
from
    raw_pulls
