# FinOps Pipeline

Automated cloud cost reporting pipeline for AWS and GCP spend analysis.
Runs daily via Airflow, loads to BigQuery, and powers the executive cost dashboard.

## Overview
- Ingests billing exports from AWS Cost Explorer and GCP Billing API
- Normalises currency, tags, and cost centres across clouds
- Loads enriched data to `finops.daily_spend` in BigQuery
- Triggers Looker dashboard refresh on completion

## Architecture
- Orchestration: Apache Airflow 2.7 (Cloud Composer)
- Transforms: dbt Core (models in /models)
- Storage: BigQuery dataset `finops`
- Notifications: Slack alerts on failure via PagerDuty webhook

## Owners
- Data Engineering: Sarah Chen (sarah@company.com)
- Finance stakeholder: James Okafor (james@company.com)

## Known Issues
- AWS Cost Explorer API has a 24hr lag — yesterday's data arrives at 09:00 UTC
- GCP committed use discount data occasionally duplicates on month boundaries
- Airflow task `normalize_tags` flaky on Mondays due to upstream tag refresh

## Runbook
On pipeline failure:
1. Check Airflow logs in Cloud Composer console
2. Re-run failed task with `airflow tasks run finops_daily <task_id> <date>`
3. If BigQuery load fails, check `finops_pipeline_errors` table for row-level errors
4. Escalate to Sarah Chen if not resolved within 2 hours# finops-pipeline
Mock application meant for FinOps pipeline
