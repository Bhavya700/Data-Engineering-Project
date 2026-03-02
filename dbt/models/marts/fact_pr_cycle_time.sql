{{
    config(
        materialized='incremental',
        unique_key=['repo_full_name', 'pr_number'],
        incremental_strategy='merge',
        partition_by={"field": "created_date", "data_type": "date"},
        cluster_by=['repo_full_name', 'pull_request_state']
    )
}}

with lifecycle as (
    select *
    from {{ ref('int_pr_lifecycle') }}
    {% if is_incremental() %}
    where updated_at >= (
        select coalesce(timestamp_sub(max(updated_at), interval 1 day), timestamp('1970-01-01'))
        from {{ this }}
    )
    {% endif %}
)
select
    repo_full_name,
    pr_number,
    pull_request_id,
    pull_request_state,
    pull_request_title,
    author_login,
    author_id,
    is_draft,
    additions_count,
    deletions_count,
    changed_files_count,
    comments_count,
    review_comments_count,
    commits_count,
    created_at,
    date(created_at) as created_date,
    updated_at,
    closed_at,
    merged_at,
    first_review_at,
    review_count,
    approvals_count,
    changes_requested_count,
    commented_count,
    unique_reviewer_count,
    time_to_first_review_hours,
    time_to_merge_hours,
    time_open_hours,
    is_merged
from lifecycle
