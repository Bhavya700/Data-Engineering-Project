{{ config(materialized='table', cluster_by=['repo_full_name']) }}

with repo_list as (
    select distinct repo_full_name
    from {{ ref('fact_pr_cycle_time') }}
    union distinct
    select distinct repo_full_name
    from {{ ref('fact_issue_aging') }}
),
bounds as (
    select
        coalesce(
            (
                select least(
                    coalesce(min(date(created_at)), current_date()),
                    coalesce((select min(date(created_at)) from {{ ref('fact_issue_aging') }}), current_date())
                )
                from {{ ref('fact_pr_cycle_time') }}
            ),
            current_date()
        ) as min_date,
        current_date() as max_date
),
date_spine as (
    select metric_date
    from bounds, unnest(generate_date_array(min_date, max_date)) as metric_date
),
calendar as (
    select
        d.metric_date,
        r.repo_full_name
    from date_spine d
    cross join repo_list r
),
pr_created_daily as (
    select
        date(created_at) as metric_date,
        repo_full_name,
        count(*) as prs_created_count,
        avg(time_to_first_review_hours) as avg_time_to_first_review_hours
    from {{ ref('fact_pr_cycle_time') }}
    group by 1, 2
),
pr_merged_daily as (
    select
        date(merged_at) as metric_date,
        repo_full_name,
        count(*) as prs_merged_count,
        avg(time_to_merge_hours) as avg_time_to_merge_hours
    from {{ ref('fact_pr_cycle_time') }}
    where merged_at is not null
    group by 1, 2
),
issue_snapshot_daily as (
    select
        c.metric_date,
        c.repo_full_name,
        countif(
            i.created_at is not null
            and date(i.created_at) <= c.metric_date
            and (i.closed_at is null or date(i.closed_at) > c.metric_date)
        ) as open_issues_count,
        countif(
            i.created_at is not null
            and date(i.created_at) <= c.metric_date
            and (i.closed_at is null or date(i.closed_at) > c.metric_date)
            and date_diff(c.metric_date, date(i.updated_at), day) > {{ var('stale_days', 30) }}
        ) as stale_issues_count
    from calendar c
    left join {{ ref('fact_issue_aging') }} i
      on c.repo_full_name = i.repo_full_name
    group by 1, 2
)
select
    c.metric_date,
    c.repo_full_name,
    coalesce(pcd.prs_created_count, 0) as prs_created_count,
    coalesce(pmd.prs_merged_count, 0) as prs_merged_count,
    coalesce(isd.open_issues_count, 0) as open_issues_count,
    coalesce(isd.stale_issues_count, 0) as stale_issues_count,
    pcd.avg_time_to_first_review_hours,
    pmd.avg_time_to_merge_hours
from calendar c
left join pr_created_daily pcd
  on c.metric_date = pcd.metric_date
 and c.repo_full_name = pcd.repo_full_name
left join pr_merged_daily pmd
  on c.metric_date = pmd.metric_date
 and c.repo_full_name = pmd.repo_full_name
left join issue_snapshot_daily isd
  on c.metric_date = isd.metric_date
 and c.repo_full_name = isd.repo_full_name
