{{
    config(
        materialized='incremental',
        unique_key=['repo_full_name', 'pr_number'],
        incremental_strategy='merge',
        partition_by={"field": "updated_at", "data_type": "timestamp", "granularity": "day"},
        cluster_by=['repo_full_name', 'pull_request_state']
    )
}}

with pulls as (
    select *
    from {{ ref('stg_github_pulls') }}
    {% if is_incremental() %}
    where updated_at >= (
        select coalesce(timestamp_sub(max(updated_at), interval 1 day), timestamp('1970-01-01'))
        from {{ this }}
    )
    {% endif %}
),
review_agg as (
    select *
    from {{ ref('int_pr_reviews_agg') }}
),
final as (
    select
        p.repo_full_name,
        p.pr_number,
        p.pull_request_id,
        p.pull_request_state,
        p.pull_request_title,
        p.author_login,
        p.author_id,
        p.is_draft,
        p.additions_count,
        p.deletions_count,
        p.changed_files_count,
        p.comments_count,
        p.review_comments_count,
        p.commits_count,
        p.created_at,
        p.updated_at as pr_updated_at,
        p.closed_at,
        p.merged_at,
        r.first_review_at,
        coalesce(r.review_count, 0) as review_count,
        coalesce(r.approvals_count, 0) as approvals_count,
        coalesce(r.changes_requested_count, 0) as changes_requested_count,
        coalesce(r.commented_count, 0) as commented_count,
        coalesce(r.unique_reviewer_count, 0) as unique_reviewer_count,
        case
            when r.first_review_at is not null and p.created_at is not null
                then timestamp_diff(r.first_review_at, p.created_at, hour)
        end as time_to_first_review_hours,
        case
            when p.merged_at is not null and p.created_at is not null
                then timestamp_diff(p.merged_at, p.created_at, hour)
        end as time_to_merge_hours,
        case
            when p.closed_at is not null and p.merged_at is null and p.created_at is not null
                then timestamp_diff(p.closed_at, p.created_at, hour)
        end as time_open_hours,
        p.merged_at is not null as is_merged,
        greatest(p.updated_at, coalesce(r.updated_at, timestamp('1970-01-01'))) as updated_at
    from pulls p
    left join review_agg r
      on p.repo_full_name = r.repo_full_name
     and p.pr_number = r.pr_number
)
select *
from final
