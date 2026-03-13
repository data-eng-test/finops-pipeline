"""
transform/normalize.py
----------------------
Normalises cost centre tags across AWS and GCP raw billing data.
Maps legacy tag values to canonical cost centre names defined in
the Finance team's cost centre registry.

Known issue: This task is flaky on Mondays because the cost centre
registry (upstream_tags table) is refreshed Sunday night and sometimes
isn't complete before our 06:00 UTC run. See GitHub issue #3.

Usage:
    python transform/normalize.py --date 2024-11-15
"""

import argparse
from google.cloud import bigquery

PROJECT = "my-company-prod"

# Canonical cost centre mapping — maintained by Finance team
# Source of truth: finance team Notion doc
COST_CENTRE_MAP = {
    "eng-platform":    "Engineering / Platform",
    "eng-data":        "Engineering / Data",
    "eng-backend":     "Engineering / Backend",
    "finance-ops":     "Finance / Operations",
    "marketing-growth":"Marketing / Growth",
    "untagged":        "Unallocated",
    # Legacy aliases
    "platform":        "Engineering / Platform",
    "data":            "Engineering / Data",
    "backend":         "Engineering / Backend",
}


def normalize_tags(date_str: str) -> None:
    client = bigquery.Client(project=PROJECT)

    # Build CASE statement from mapping dict
    case_clauses = "\n".join([
        f"    WHEN cost_centre = '{k}' THEN '{v}'"
        for k, v in COST_CENTRE_MAP.items()
    ])

    query = f"""
        UPDATE `{PROJECT}.raw.aws_billing`
        SET cost_center_tag = CASE
        {case_clauses}
            ELSE cost_center_tag
        END
        WHERE export_date = '{date_str}'
    """

    client.query(query).result()
    print(f"Normalised AWS tags for {date_str}")

    # Same for GCP
    query_gcp = f"""
        UPDATE `{PROJECT}.raw.gcp_billing`
        SET label_cost_centre = CASE
        {case_clauses}
            ELSE label_cost_centre
        END
        WHERE usage_start_date = '{date_str}'
    """

    client.query(query_gcp).result()
    print(f"Normalised GCP tags for {date_str}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    normalize_tags(args.date)
