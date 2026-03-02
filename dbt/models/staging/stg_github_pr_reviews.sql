{{ config(materialized='view') }}

with source_data as (
    select
        ingestion_date,
        repo_full_name,
        pr_number,
        event_type,
        payload
    from {{ source('github', 'github_pr_reviews_raw') }}
    where event_type = 'pr_review'
),
parsed as (
    select
        ingestion_date,
        repo_full_name,
        coalesce(
            pr_number,
            safe_cast(regexp_extract(json_value(payload, '$.pull_request_url'), r'/pulls/(\d+)$') as int64)
        ) as pr_number,
        safe_cast(json_value(payload, '$.id') as int64) as review_id,
        json_value(payload, '$.node_id') as review_node_id,
        upper(json_value(payload, '$.state')) as review_state,
        json_value(payload, '$.body') as review_body,
        json_value(payload, '$.html_url') as review_url,
        json_value(payload, '$.user.login') as reviewer_login,
        safe_cast(json_value(payload, '$.user.id') as int64) as reviewer_id,
        safe_cast(json_value(payload, '$.submitted_at') as timestamp) as submitted_at,
        safe_cast(json_value(payload, '$.created_at') as timestamp) as created_at,
        safe_cast(json_value(payload, '$.updated_at') as timestamp) as source_updated_at
    from source_data
),
normalized as (
    select
        ingestion_date,
        repo_full_name,
        pr_number,
        review_id,
        review_node_id,
        review_state,
        review_body,
        review_url,
        reviewer_login,
        reviewer_id,
        submitted_at,
        created_at,
        coalesce(submitted_at, source_updated_at, created_at) as updated_at
    from parsed
),
deduped as (
    select *
    from normalized
    qualify row_number() over (
        partition by repo_full_name, review_id
        order by updated_at desc nulls last, ingestion_date desc
    ) = 1
)
select *
from deduped
where review_id is not null
  and pr_number is not null
