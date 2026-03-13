-- dbt model: daily_spend
-- Final curated table powering the FinOps Looker dashboard.
-- Target: finops.daily_spend in BigQuery
-- Refresh: daily at ~07:00 UTC (after finops_daily DAG completes)
-- Owner: Sarah Chen (sarah@company.com)

{{
  config(
    materialized = 'table',
    partition_by = { "field": "date", "data_type": "date" },
    cluster_by  = ["cloud_provider", "cost_centre"],
    tags        = ["finops", "daily"]
  )
}}

SELECT
    date,
    cloud_provider,
    cost_centre,
    service_name,
    account_id,
    SUM(cost_usd)        AS total_cost_usd,
    SUM(committed_cost)  AS committed_usd,
    SUM(on_demand_cost)  AS on_demand_usd,
    COUNT(*)             AS row_count
FROM {{ ref('stg_billing_raw') }}
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 13 MONTH)  -- rolling 13 months
GROUP BY 1, 2, 3, 4, 5
