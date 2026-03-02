{{ config(materialized='view') }}

with source_data as (
    select
        ingestion_date,
        repo_full_name,
        event_type,
        payload
    from {{ source('github', 'github_pulls_raw') }}
    where event_type = 'pull'
),
parsed as (
    select
        ingestion_date,
        repo_full_name,
        safe_cast(json_value(payload, '$.number') as int64) as pr_number,
        safe_cast(json_value(payload, '$.id') as int64) as pull_request_id,
        json_value(payload, '$.node_id') as pull_request_node_id,
        lower(json_value(payload, '$.state')) as pull_request_state,
        json_value(payload, '$.title') as pull_request_title,
        safe_cast(json_value(payload, '$.draft') as bool) as is_draft,
        json_value(payload, '$.html_url') as pull_request_url,
        json_value(payload, '$.base.ref') as base_branch,
        json_value(payload, '$.head.ref') as head_branch,
        json_value(payload, '$.user.login') as author_login,
        safe_cast(json_value(payload, '$.user.id') as int64) as author_id,
        safe_cast(json_value(payload, '$.additions') as int64) as additions_count,
        safe_cast(json_value(payload, '$.deletions') as int64) as deletions_count,
        safe_cast(json_value(payload, '$.changed_files') as int64) as changed_files_count,
        safe_cast(json_value(payload, '$.comments') as int64) as comments_count,
        safe_cast(json_value(payload, '$.review_comments') as int64) as review_comments_count,
        safe_cast(json_value(payload, '$.commits') as int64) as commits_count,
        safe_cast(json_value(payload, '$.created_at') as timestamp) as created_at,
        safe_cast(json_value(payload, '$.updated_at') as timestamp) as updated_at,
        safe_cast(json_value(payload, '$.closed_at') as timestamp) as closed_at,
        safe_cast(json_value(payload, '$.merged_at') as timestamp) as merged_at
    from source_data
),
deduped as (
    select *
    from parsed
    qualify row_number() over (
        partition by repo_full_name, pr_number
        order by updated_at desc nulls last, ingestion_date desc
    ) = 1
)
select *
from deduped
where pr_number is not null
