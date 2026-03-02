{{ config(materialized='view') }}

with source_data as (
    select
        ingestion_date,
        repo_full_name,
        event_type,
        payload
    from {{ source('github', 'github_issues_raw') }}
    where event_type = 'issue'
      and json_query(payload, '$.pull_request') is null
),
parsed as (
    select
        ingestion_date,
        repo_full_name,
        safe_cast(json_value(payload, '$.number') as int64) as issue_number,
        safe_cast(json_value(payload, '$.id') as int64) as issue_id,
        json_value(payload, '$.node_id') as issue_node_id,
        lower(json_value(payload, '$.state')) as issue_state,
        json_value(payload, '$.title') as issue_title,
        json_value(payload, '$.html_url') as issue_url,
        json_value(payload, '$.user.login') as author_login,
        safe_cast(json_value(payload, '$.user.id') as int64) as author_id,
        safe_cast(json_value(payload, '$.comments') as int64) as comments_count,
        coalesce(array_length(json_query_array(payload, '$.labels')), 0) as labels_count,
        array_to_string(
            array(
                select json_value(label, '$.name')
                from unnest(coalesce(json_query_array(payload, '$.labels'), cast([] as array<json>))) as label
                where json_value(label, '$.name') is not null
            ),
            ','
        ) as labels_csv,
        safe_cast(json_value(payload, '$.created_at') as timestamp) as created_at,
        safe_cast(json_value(payload, '$.updated_at') as timestamp) as updated_at,
        safe_cast(json_value(payload, '$.closed_at') as timestamp) as closed_at
    from source_data
),
deduped as (
    select *
    from parsed
    qualify row_number() over (
        partition by repo_full_name, issue_number
        order by updated_at desc nulls last, ingestion_date desc
    ) = 1
)
select *
from deduped
where issue_number is not null
