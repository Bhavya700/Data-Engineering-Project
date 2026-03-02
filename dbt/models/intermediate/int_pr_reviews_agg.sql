{{
    config(
        materialized='incremental',
        unique_key=['repo_full_name', 'pr_number'],
        incremental_strategy='merge',
        partition_by={"field": "updated_at", "data_type": "timestamp", "granularity": "day"},
        cluster_by=['repo_full_name']
    )
}}

with reviews as (
    select *
    from {{ ref('stg_github_pr_reviews') }}
    {% if is_incremental() %}
    where updated_at >= (
        select coalesce(timestamp_sub(max(updated_at), interval 1 day), timestamp('1970-01-01'))
        from {{ this }}
    )
    {% endif %}
),
aggregated as (
    select
        repo_full_name,
        pr_number,
        min(submitted_at) as first_review_at,
        count(*) as review_count,
        countif(review_state = 'APPROVED') as approvals_count,
        countif(review_state = 'CHANGES_REQUESTED') as changes_requested_count,
        countif(review_state = 'COMMENTED') as commented_count,
        count(distinct reviewer_login) as unique_reviewer_count,
        max(updated_at) as updated_at
    from reviews
    group by 1, 2
)
select *
from aggregated
