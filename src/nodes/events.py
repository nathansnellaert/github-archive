"""GitHub Archive Events.

Downloads and transforms GitHub event data from GH Archive.
Aggregates by hour for past 30 days.
"""
import gzip
import json
from datetime import datetime, timedelta
from collections import Counter

import pyarrow as pa
from subsets_utils import get, save_raw_json, load_raw_json, load_state, save_state, merge, validate, publish
from subsets_utils.testing import assert_matches_pattern, assert_positive


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


def download():
    """Fetch GitHub event data from GH Archive - aggregate by hour for past 30 days."""
    print("Downloading GitHub events...")
    state = load_state("github")
    processed_dates = set(state.get("processed_dates", []))

    end_time = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(days=30)

    # Load existing aggregates or start fresh
    hourly_counts = Counter()

    current = start_time
    dates_to_process = []

    # Collect dates that need processing
    while current < end_time:
        date_str = current.strftime("%Y-%m-%d")
        if date_str not in processed_dates:
            dates_to_process.append(date_str)
        current += timedelta(days=1)

    dates_to_process = list(set(dates_to_process))
    print(f"  Processing {len(dates_to_process)} days of GitHub data...")

    for date_str in sorted(dates_to_process):
        print(f"  Fetching {date_str}...")
        day_events = 0

        for hour in range(24):
            url = f"https://data.gharchive.org/{date_str}-{hour}.json.gz"

            try:
                response = get(url)
                if response.status_code == 200:
                    decompressed = gzip.decompress(response.content)
                    lines = decompressed.decode('utf-8').strip().split('\n')

                    hour_key = f"{date_str}T{hour:02d}:00:00Z"

                    for line in lines:
                        if line:
                            event = json.loads(line)
                            event_type = event.get("type", "Unknown")
                            hourly_counts[(hour_key, event_type)] += 1
                            day_events += 1
            except Exception as e:
                print(f"    Hour {hour} failed: {e}")

        print(f"    -> {day_events:,} events")

        # Mark date as processed
        processed_dates.add(date_str)
        save_state("github", {"processed_dates": list(processed_dates)})

    # Convert to list format
    records = [
        {"hour": hour, "event_type": event_type, "event_count": count}
        for (hour, event_type), count in sorted(hourly_counts.items())
    ]

    print(f"  Total: {len(records)} hourly event type records")
    save_raw_json(records, "github_hourly_aggregates")


def test(table: pa.Table) -> None:
    """Validate GitHub activity output."""
    validate(table, {
        "columns": {
            "hour": "string",
            "event_type": "string",
            "event_count": "int",
        },
        "not_null": ["hour", "event_type", "event_count"],
        "unique": ["hour", "event_type"],
        "min_rows": 1,
    })

    # Hour format check
    assert_matches_pattern(table, "hour", r"^\d{4}-\d{2}-\d{2}T\d{2}:00:00Z$", "hourly timestamp")

    # Event counts must be positive
    assert_positive(table, "event_count", allow_zero=False)

    # Known GitHub event types
    known_types = {
        "PushEvent", "PullRequestEvent", "IssuesEvent", "WatchEvent",
        "ForkEvent", "CreateEvent", "DeleteEvent", "IssueCommentEvent",
        "PullRequestReviewEvent", "PullRequestReviewCommentEvent",
        "CommitCommentEvent", "GollumEvent", "MemberEvent", "PublicEvent",
        "ReleaseEvent", "SponsorshipEvent"
    }
    event_types = set(table.column("event_type").to_pylist())
    unknown = event_types - known_types
    if unknown:
        print(f"    Note: Found new event types: {unknown}")


def transform():
    """Transform pre-aggregated GitHub hourly data."""
    print("Transforming GitHub events...")
    # Ingest already aggregates to reduce memory - just load and upload
    records = load_raw_json("github_hourly_aggregates")
    print(f"  Loaded {len(records)} hourly aggregates")

    table = pa.Table.from_pylist(records)
    print(f"  {len(table):,} hourly event type records")

    test(table)

    merge(table, DATASET_ID, key=["hour", "event_type"])
    publish(DATASET_ID, METADATA)
def run():
    """Download and transform GitHub events."""
    download()
    transform()


NODES = {
    run: [],
}


if __name__ == "__main__":
    from subsets_utils import validate_environment
    validate_environment()
    run()
