import pyarrow as pa
from subsets_utils import load_raw_json, upload_data, publish
from .test import test

DATASET_ID = "github_activity_hourly"

METADATA = {
    "id": DATASET_ID,
    "title": "GitHub Activity (Hourly)",
    "description": "Hourly aggregated GitHub activity metrics from GH Archive (30-day rolling window). Tracks commits, pull requests, issues, stars, and other events across all public repositories.",
    "column_descriptions": {
        "hour": "Hour timestamp (YYYY-MM-DDTHH:00:00Z)",
        "event_type": "Type of GitHub event (PushEvent, PullRequestEvent, etc.)",
        "event_count": "Number of events in this hour",
    }
}


def run():
    """Transform pre-aggregated GitHub hourly data."""
    # Ingest already aggregates to reduce memory - just load and upload
    records = load_raw_json("github_hourly_aggregates")
    print(f"  Loaded {len(records)} hourly aggregates")

    table = pa.Table.from_pylist(records)
    print(f"  {len(table):,} hourly event type records")

    test(table)

    upload_data(table, DATASET_ID, mode="merge", merge_key=["hour", "event_type"])
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
