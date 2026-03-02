select *
from {{ ref('fact_pr_cycle_time') }}
where created_at is null
   or created_at > current_timestamp()
   or (updated_at is not null and updated_at < created_at)
   or (closed_at is not null and closed_at < created_at)
   or (merged_at is not null and merged_at < created_at)
