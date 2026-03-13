# Architecture

## Overview

The FinOps pipeline ingests cloud billing data from AWS and GCP daily,
transforms it using dbt, and loads the results to BigQuery to power
the executive cost dashboard in Looker.

## Data Flow

```
AWS Cost Explorer API
        |
   ingest/aws.py  ──────────────────────────────┐
                                                  ▼
                                        raw.aws_billing (BigQuery)
                                                  │
                                                  ▼
GCP Billing Export (BigQuery)          stg_billing_raw (dbt)
        │                                         │
   ingest/gcp.py ──► raw.gcp_billing ────────────┘
                                                  │
                                                  ▼
                                      finops.daily_spend (dbt)
                                                  │
                                                  ▼
                                       Looker FinOps Dashboard
```

## BigQuery Datasets

| Dataset    | Purpose                        | Access               |
|------------|--------------------------------|----------------------|
| raw        | Landing zone — untransformed   | Pipeline SA only     |
| finops     | Curated, business-ready        | Finance + Data leads |
| _sandbox   | Dev/test — not production      | All data engineers   |

## GCP Projects

| Environment | Project ID             | Branch   | Deployed by        |
|-------------|------------------------|----------|--------------------|
| Production  | my-company-prod        | main     | CI/CD on merge     |
| Staging     | my-company-staging     | staging  | CI/CD on merge     |
| Dev         | my-company-dev         | develop  | Manual / local     |

## Orchestration

- Platform: Apache Airflow 2.7 on Cloud Composer (GCP managed)
- DAG: `finops_daily` — runs at 06:00 UTC every day
- SLA: data available in BigQuery by 07:30 UTC
- Alerting: Slack #data-alerts channel + PagerDuty for SLA breaches

## Transformations

- Tool: dbt Core (run via BashOperator in Airflow)
- Models: `stg_billing_raw` → `daily_spend`
- Tests: not_null, accepted_values on all key columns
- Docs: `dbt docs generate` — hosted internally at data-docs.company.com

## Dependencies

| Dependency            | Version | Notes                               |
|-----------------------|---------|-------------------------------------|
| Apache Airflow        | 2.7.3   | Managed by Cloud Composer           |
| dbt Core              | 1.7.x   | Installed in Composer environment   |
| google-cloud-bigquery | 3.x     | Python client for BQ loads          |
| boto3                 | 1.34.x  | AWS Cost Explorer API client        |

## Access Control

- Read access to `finops` dataset: Finance team, Data team leads
- Write access: `finops-pipeline@my-company-prod.iam.gserviceaccount.com` only
- Airflow UI: Data Engineering team members (Google SSO)
- Looker: Finance team + senior leadership
