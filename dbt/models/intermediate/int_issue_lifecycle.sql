{{
    config(
        materialized='incremental',
        unique_key=['repo_full_name', 'issue_number'],
        incremental_strategy='merge',
        partition_by={"field": "updated_at", "data_type": "timestamp", "granularity": "day"},
        cluster_by=['repo_full_name', 'issue_state']
    )
}}

with issues as (
    select *
    from {{ ref('stg_github_issues') }}
    {% if is_incremental() %}
    where updated_at >= (
        select coalesce(timestamp_sub(max(updated_at), interval 1 day), timestamp('1970-01-01'))
        from {{ this }}
    )
    {% endif %}
),
final as (
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
        updated_at,
        closed_at,
        case
            when closed_at is not null and created_at is not null
                then timestamp_diff(closed_at, created_at, hour)
        end as time_to_close_hours,
        case
            when closed_at is not null and created_at is not null
                then date_diff(date(closed_at), date(created_at), day)
            when created_at is not null
                then date_diff(current_date(), date(created_at), day)
        end as issue_age_days,
        case
            when issue_state = 'open'
             and updated_at is not null
             and date_diff(current_date(), date(updated_at), day) > {{ var('stale_days', 30) }}
                then true
            else false
        end as is_stale
    from issues
)
select *
from final
