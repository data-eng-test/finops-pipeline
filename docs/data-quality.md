# Data Quality

## dbt Tests

All models have dbt tests defined in `models/schema.yml`.
Run with: `dbt test --select finops --target prod`

| Test                          | Model            | Column         | Action on failure     |
|-------------------------------|------------------|----------------|-----------------------|
| not_null                      | stg_billing_raw  | date           | Block pipeline        |
| not_null                      | stg_billing_raw  | cost_usd       | Block pipeline        |
| accepted_values (AWS, GCP)    | stg_billing_raw  | cloud_provider | Block pipeline        |
| not_null                      | daily_spend      | date           | Block pipeline        |

## Row Count Checks

The `load_bq.py` script validates row counts before marking a run as successful:

- AWS rows for a given date must be > 500 (alert if lower — possible export issue)
- GCP rows for a given date must be > 200
- Total daily_spend rows must increase day-over-day (alert if flat or decreasing)

## Known Data Quality Issues

### GCP Duplicate Rows on Month Boundary
- **Issue:** Committed use discount rows occasionally duplicated on the 1st of each month
- **Workaround:** `stg_billing_raw.sql` deduplicates using QUALIFY + ROW_NUMBER
- **Status:** Open — root cause not confirmed (possibly GCP Billing API bug)
- **GitHub Issue:** #1

### AWS 24-Hour Lag
- **Issue:** AWS Cost Explorer data for date D is not available until D+1 09:00 UTC
- **Impact:** Yesterday's AWS spend is always estimated until the following morning
- **Workaround:** Pipeline runs at 06:00 UTC using D-1 data — acceptable for reporting

### Tag Coverage
- **Issue:** ~12% of AWS resources have no CostCenter tag
- **Impact:** Those costs land in `cost_centre = 'untagged'` in the dashboard
- **Owner:** Finance team to chase resource owners for tag compliance
- **Tracking:** FINOPS-89 in Jira

## Monitoring

- Airflow task failures → Slack #data-alerts immediately
- SLA miss (data not in BQ by 07:30 UTC) → PagerDuty → on-call engineer
- Dashboard: Datadog pipeline health board (link in Notion)
