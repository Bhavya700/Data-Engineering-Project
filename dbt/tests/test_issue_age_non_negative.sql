select *
from {{ ref('fact_issue_aging') }}
where (issue_age_days is not null and issue_age_days < 0)
   or (time_to_close_hours is not null and time_to_close_hours < 0)
