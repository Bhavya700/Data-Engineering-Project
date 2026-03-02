select
    r.repo_full_name,
    r.pr_number,
    r.review_id,
    r.submitted_at,
    p.created_at
from {{ ref('stg_github_pr_reviews') }} r
join {{ ref('stg_github_pulls') }} p
  on r.repo_full_name = p.repo_full_name
 and r.pr_number = p.pr_number
where r.submitted_at is not null
  and p.created_at is not null
  and r.submitted_at < p.created_at
