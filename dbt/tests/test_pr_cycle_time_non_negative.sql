select *
from {{ ref('fact_pr_cycle_time') }}
where (time_to_first_review_hours is not null and time_to_first_review_hours < 0)
   or (time_to_merge_hours is not null and time_to_merge_hours < 0)
   or (time_open_hours is not null and time_open_hours < 0)
