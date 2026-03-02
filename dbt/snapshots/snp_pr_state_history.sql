{% snapshot snp_pr_state_history %}

{{
    config(
        strategy='timestamp',
        updated_at='updated_at',
        unique_key="concat(repo_full_name, '-', cast(pr_number as string))",
        invalidate_hard_deletes=True
    )
}}

select
    repo_full_name,
    pr_number,
    pull_request_id,
    pull_request_state,
    pull_request_title,
    author_login,
    is_draft,
    created_at,
    updated_at,
    closed_at,
    merged_at
from {{ ref('stg_github_pulls') }}

{% endsnapshot %}
