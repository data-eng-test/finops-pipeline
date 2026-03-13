"""
ingest/aws.py
-------------
Fetches AWS Cost Explorer billing data for a given date and loads it
to BigQuery raw.aws_billing.

Usage:
    python ingest/aws.py --date 2024-11-15

Dependencies:
    boto3, google-cloud-bigquery

AWS permissions required:
    ce:GetCostAndUsage on the management account
"""

import argparse
import boto3
from google.cloud import bigquery
from datetime import datetime, timedelta

BQ_PROJECT  = "my-company-prod"
BQ_DATASET  = "raw"
BQ_TABLE    = "aws_billing"
AWS_REGION  = "us-east-1"


def fetch_cost_explorer(date_str: str) -> list[dict]:
    """Fetch daily cost breakdown from AWS Cost Explorer."""
    client = boto3.client("ce", region_name=AWS_REGION)

    start = date_str
    end   = (datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

    response = client.get_cost_and_usage(
        TimePeriod={"Start": start, "End": end},
        Granularity="DAILY",
        Metrics=["UnblendedCost"],
        GroupBy=[
            {"Type": "DIMENSION", "Key": "SERVICE"},
            {"Type": "DIMENSION", "Key": "LINKED_ACCOUNT"},
            {"Type": "TAG",       "Key": "CostCenter"},
        ],
    )

    rows = []
    for result in response.get("ResultsByTime", []):
        for group in result.get("Groups", []):
            service, account, cost_centre = group["Keys"]
            rows.append({
                "export_date":               date_str,
                "service_name":              service,
                "linked_account_id":         account,
                "cost_center_tag":           cost_centre.replace("CostCenter$", "") or "untagged",
                "line_item_unblended_cost":  float(group["Metrics"]["UnblendedCost"]["Amount"]),
                "line_item_type":            "Usage",
                "line_item_resource_id":     f"{account}/{service}",
            })
    return rows


def load_to_bigquery(rows: list[dict], date_str: str) -> None:
    """Load rows to BigQuery, replacing existing data for that date."""
    client = bigquery.Client(project=BQ_PROJECT)
    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    # Delete existing rows for this date first (idempotent reload)
    client.query(f"DELETE FROM `{table_ref}` WHERE export_date = '{date_str}'").result()

    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")

    print(f"Loaded {len(rows)} rows to {table_ref} for {date_str}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Date to ingest (YYYY-MM-DD)")
    args = parser.parse_args()

    rows = fetch_cost_explorer(args.date)
    if not rows:
        print(f"WARNING: No rows returned from Cost Explorer for {args.date}")
    else:
        load_to_bigquery(rows, args.date)
