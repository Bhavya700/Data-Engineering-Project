-- Adapted from fct_orders_date_valid.sql
select
    *
from
    {{ ref('stg_github_pulls') }}
where
    -- Using BigQuery standard SQL date functions
    DATE(created_at) > CURRENT_DATE()
    or DATE(created_at) < DATE('1990-01-01')
