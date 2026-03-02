{{
    config(
        materialized='incremental',
        unique_key=['repo_full_name', 'issue_number'],
        incremental_strategy='merge',
        partition_by={"field": "created_date", "data_type": "date"},
        cluster_by=['repo_full_name', 'issue_state']
    )
}}

with lifecycle as (
    select *
    from {{ ref('int_issue_lifecycle') }}
    {% if is_incremental() %}
    where updated_at >= (
        select coalesce(timestamp_sub(max(updated_at), interval 1 day), timestamp('1970-01-01'))
        from {{ this }}
    )
    {% endif %}
)
select
    repo_full_name,
    issue_number,
    issue_id,
    issue_node_id,
    issue_state,
    issue_title,
    issue_url,
    author_login,
    author_id,
    comments_count,
    labels_count,
    labels_csv,
    created_at,
    date(created_at) as created_date,
    updated_at,
    date(updated_at) as updated_date,
    closed_at,
    date(closed_at) as closed_date,
    time_to_close_hours,
    issue_age_days,
    is_stale
from lifecycle
