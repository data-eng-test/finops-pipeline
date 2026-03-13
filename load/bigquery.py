"""
load/bigquery.py
----------------
Final step in the finops_daily DAG.
Validates row counts and marks the pipeline run as complete
in the pipeline_runs audit table.

Usage:
    python load/bigquery.py --date 2024-11-15
"""

import argparse
from google.cloud import bigquery
from datetime import datetime

PROJECT   = "my-company-prod"
AUDIT_TABLE = f"{PROJECT}.finops.pipeline_runs"

# Minimum expected row counts — alert if below these
ROW_COUNT_THRESHOLDS = {
    "aws_billing": 500,
    "gcp_billing": 200,
}


def validate_row_counts(client: bigquery.Client, date_str: str) -> list[str]:
    """Returns list of warning messages if row counts are below threshold."""
    warnings = []
    for table, threshold in ROW_COUNT_THRESHOLDS.items():
        result = client.query(
            f"SELECT COUNT(*) as cnt FROM `{PROJECT}.raw.{table}` "
            f"WHERE DATE(COALESCE(export_date, usage_start_date)) = '{date_str}'"
        ).result()
        count = list(result)[0].cnt
        if count < threshold:
            warnings.append(f"{table}: only {count} rows (expected ≥ {threshold})")
    return warnings


def record_pipeline_run(client: bigquery.Client, date_str: str, status: str, warnings: list) -> None:
    rows = [{
        "run_date":     date_str,
        "pipeline":     "finops_daily",
        "status":       status,
        "warnings":     "; ".join(warnings) if warnings else None,
        "completed_at": datetime.utcnow().isoformat(),
    }]
    errors = client.insert_rows_json(AUDIT_TABLE, rows)
    if errors:
        print(f"WARNING: Could not write to audit table: {errors}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()

    client = bigquery.Client(project=PROJECT)
    warnings = validate_row_counts(client, args.date)

    if warnings:
        for w in warnings:
            print(f"WARNING: {w}")
        record_pipeline_run(client, args.date, "warning", warnings)
    else:
        record_pipeline_run(client, args.date, "success", [])
        print(f"Pipeline run for {args.date} completed successfully.")
