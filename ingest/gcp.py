"""
ingest/gcp.py
-------------
Reads GCP Billing Export from BigQuery source project and copies
the relevant date partition to raw.gcp_billing in our data platform.

Usage:
    python ingest/gcp.py --date 2024-11-15

GCP permissions required:
    bigquery.tables.getData on the billing export project
    bigquery.tables.create  on my-company-prod.raw
"""

import argparse
from google.cloud import bigquery

SRC_PROJECT  = "my-company-billing"
SRC_DATASET  = "billing_export"
SRC_TABLE    = "gcp_billing_export_v1"

DST_PROJECT  = "my-company-prod"
DST_DATASET  = "raw"
DST_TABLE    = "gcp_billing"


def copy_billing_partition(date_str: str) -> None:
    """Copy a single date partition from GCP billing export to raw dataset."""
    client = bigquery.Client(project=DST_PROJECT)

    src = f"`{SRC_PROJECT}.{SRC_DATASET}.{SRC_TABLE}`"
    dst = f"{DST_PROJECT}.{DST_DATASET}.{DST_TABLE}"

    # Delete existing rows for this date (idempotent)
    client.query(f"DELETE FROM `{dst}` WHERE usage_start_date = '{date_str}'").result()

    # Copy from billing export, flattening labels array to key-value
    query = f"""
        INSERT INTO `{dst}`
        SELECT
            DATE(usage_start_time)                      AS usage_start_date,
            service.description                         AS service_description,
            sku.description                             AS sku_description,
            project.id                                  AS project_id,
            resource.name                               AS resource_name,
            cost                                        AS cost,
            (SELECT SUM(c.amount) FROM UNNEST(credits) c
             WHERE c.type = 'COMMITTED_USAGE_DISCOUNT') AS credits_amount,
            (SELECT value FROM UNNEST(labels)
             WHERE key = 'cost-centre' LIMIT 1)         AS label_cost_centre,
            export_time
        FROM {src}
        WHERE DATE(usage_start_time) = '{date_str}'
    """

    job = client.query(query)
    result = job.result()
    print(f"Copied GCP billing rows for {date_str} to {dst}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Date to ingest (YYYY-MM-DD)")
    args = parser.parse_args()
    copy_billing_partition(args.date)
