-- dbt model: stg_billing_raw
-- Unions raw AWS and GCP billing exports into a single normalised staging table.
-- Runs before: daily_spend.sql
-- Source tables: raw.aws_billing, raw.gcp_billing

WITH aws AS (
    SELECT
        export_date                        AS date,
        'AWS'                              AS cloud_provider,
        cost_center_tag                    AS cost_centre,
        service_name,
        line_item_unblended_cost           AS cost_usd,
        0                                  AS committed_cost,
        line_item_unblended_cost           AS on_demand_cost,
        line_item_resource_id              AS resource_id,
        linked_account_id                  AS account_id
    FROM {{ source('raw', 'aws_billing') }}
    WHERE line_item_type NOT IN ('Credit', 'Refund')
),

gcp AS (
    SELECT
        usage_start_date                   AS date,
        'GCP'                              AS cloud_provider,
        COALESCE(label_cost_centre, 'untagged') AS cost_centre,
        service_description                AS service_name,
        cost                               AS cost_usd,
        credits_amount                     AS committed_cost,
        cost - COALESCE(credits_amount, 0) AS on_demand_cost,
        resource_name                      AS resource_id,
        project_id                         AS account_id
    FROM {{ source('raw', 'gcp_billing') }}
    -- Known issue: GCP duplicates committed use rows on month boundary
    -- Dedup workaround until root cause resolved (see GitHub issue #1)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY usage_start_date, resource_name, service_description
        ORDER BY export_time DESC
    ) = 1
)

SELECT * FROM aws
UNION ALL
SELECT * FROM gcp
